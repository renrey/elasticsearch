/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;

/**
 * Fetch phase of a search request, used to fetch the actual top matching documents to be returned to the client, identified
 * after reducing all of the matches returned by the query phase
 */
public class FetchPhase {
    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    private final FetchSubPhase[] fetchSubPhases;

    public FetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases.toArray(new FetchSubPhase[fetchSubPhases.size() + 1]);
        this.fetchSubPhases[fetchSubPhases.size()] = new InnerHitsPhase(this);
    }

    public void execute(SearchContext context) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(context));
        }

        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        if (context.docIdsToLoadSize() == 0) {
            // no individual hits to process, so we shortcut
            context.fetchResult().hits(new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(),
                context.queryResult().getMaxScore()));
            return;
        }

        DocIdToIndex[] docs = new DocIdToIndex[context.docIdsToLoadSize()];
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            docs[index] = new DocIdToIndex(context.docIdsToLoad()[index], index);
        }

        // 先排序-》下面通过docid二分查找
        // make sure that we iterate in doc id order
        Arrays.sort(docs);

        // key=字段类型，v=字段名集合
        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        // 验证是否存在指定字段，并设置storedToRequestedFields，返回CustomFieldsVisitor
        FieldsVisitor fieldsVisitor = createStoredFieldsVisitor(context, storedToRequestedFields);

        FetchContext fetchContext = new FetchContext(context);

        // 应该存放结果集合
        SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];

        List<FetchSubPhaseProcessor> processors = getProcessors(context.shardTarget(), fetchContext);
        NestedDocuments nestedDocuments = context.getSearchExecutionContext().getNestedDocuments();

        int currentReaderIndex = -1;
        LeafReaderContext currentReaderContext = null;
        LeafNestedDocuments leafNestedDocuments = null;
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader = null;
        boolean hasSequentialDocs = hasSequentialDocs(docs);
        // 遍历docid
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            if (context.isCancelled()) {
                throw new TaskCancelledException("cancelled");
            }
            // 指定docid
            int docId = docs[index].docId;
            try {
                // 通过docid （二分查找比较每个segment的base值，找小于当前的最大doc）找到 对应leave（segment）
                int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
                if (currentReaderIndex != readerIndex) {
                    currentReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                    currentReaderIndex = readerIndex;
                    // 下面就是从segment取出 doc的函数
                    if (currentReaderContext.reader() instanceof SequentialStoredFieldsLeafReader
                            && hasSequentialDocs && docs.length >= 10) {
                        // All the docs to fetch are adjacent but Lucene stored fields are optimized
                        // for random access and don't optimize for sequential access - except for merging.
                        // So we do a little hack here and pretend we're going to do merges in order to
                        // get better sequential access.
                        SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) currentReaderContext.reader();
                        fieldReader = lf.getSequentialStoredFieldsReader()::visitDocument;
                    } else {
                        // SingleDocDirectoryReader?
                        // SegmentReader
                        // org.apache.lucene.index.CodecReader.document

                        // .fld文件存了每个docId（就是全局顺序下标）在对应segment的文件的开始位置
                        fieldReader = currentReaderContext.reader()::document;// 文档reader
                    }
                    for (FetchSubPhaseProcessor processor : processors) {
                        processor.setNextReader(currentReaderContext);
                    }
                    // 内置类型
                    leafNestedDocuments = nestedDocuments.getLeafNestedDocuments(currentReaderContext);
                }
                assert currentReaderContext != null;
                // 就是把数据（通过reader、fieldsVisitor）加载，并返回HitContext-》当前目标docId的数据序列化
                HitContext hit = prepareHitContext(
                    context,
                    leafNestedDocuments,
                    nestedDocuments::hasNonNestedParent,
                    fieldsVisitor,
                    docId,
                    storedToRequestedFields,
                    currentReaderContext,
                    fieldReader);
                for (FetchSubPhaseProcessor processor : processors) {
                    processor.process(hit);
                }
                // 放入结果 -》
                hits[docs[index].index] = hit.hit();// 直接SearchHit
            } catch (Exception e) {
                throw new FetchPhaseExecutionException(context.shardTarget(), "Error running fetch phase for doc [" + docId + "]", e);
            }
        }
        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        // 还是用TotalHits存结果
        TotalHits totalHits = context.queryResult().getTotalHits();
        // 加入到fetchResult
        context.fetchResult().hits(new SearchHits(hits, totalHits, context.queryResult().getMaxScore()));

    }

    List<FetchSubPhaseProcessor> getProcessors(SearchShardTarget target, FetchContext context) {
        try {
            List<FetchSubPhaseProcessor> processors = new ArrayList<>();
            for (FetchSubPhase fsp : fetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(processor);
                }
            }
            return processors;
        } catch (Exception e) {
            throw new FetchPhaseExecutionException(target, "Error building fetch sub-phases", e);
        }
    }

    static class DocIdToIndex implements Comparable<DocIdToIndex> {
        final int docId;
        final int index;

        DocIdToIndex(int docId, int index) {
            this.docId = docId;
            this.index = index;
        }

        @Override
        public int compareTo(DocIdToIndex o) {
            return Integer.compare(docId, o.docId);
        }
    }

    private FieldsVisitor createStoredFieldsVisitor(SearchContext context, Map<String, Set<String>> storedToRequestedFields) {
        StoredFieldsContext storedFieldsContext = context.storedFieldsContext();

        if (storedFieldsContext == null) {
            // no fields specified, default to return source if no explicit indication
            if (context.hasScriptFields() == false && context.hasFetchSourceContext() == false) {
                context.fetchSourceContext(new FetchSourceContext(true));
            }
            boolean loadSource = sourceRequired(context);
            return new FieldsVisitor(loadSource);
        } else if (storedFieldsContext.fetchFields() == false) {
            // disable stored fields entirely
            return null;
        } else {
            // 遍历入参中字段名的范式
            for (String fieldNameOrPattern : context.storedFieldsContext().fieldNames()) {
                // _source ：应该是全部字段？
                if (fieldNameOrPattern.equals(SourceFieldMapper.NAME)) {
                    FetchSourceContext fetchSourceContext = context.hasFetchSourceContext() ? context.fetchSourceContext()
                        : FetchSourceContext.FETCH_SOURCE;
                    context.fetchSourceContext(new FetchSourceContext(true, fetchSourceContext.includes(), fetchSourceContext.excludes()));
                    continue;
                }
                SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
                Collection<String> fieldNames = searchExecutionContext.simpleMatchToIndexNames(fieldNameOrPattern);
                // 遍历需要的字段名
                for (String fieldName : fieldNames) {
                    // 查找mapping中是否存在这个字段
                    MappedFieldType fieldType = searchExecutionContext.getFieldType(fieldName);
                    if (fieldType == null) {// 不存在，异常
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        if (searchExecutionContext.getObjectMapper(fieldName) != null) {
                            throw new IllegalArgumentException("field [" + fieldName + "] isn't a leaf field");
                        }
                    } else {
                        // 存在字段
                        // 字段类型名
                        String storedField = fieldType.name();
                        // storedToRequestedFields中，放入key=字段类型，v=set
                        Set<String> requestedFields = storedToRequestedFields.computeIfAbsent(
                            storedField, key -> new HashSet<>());
                        requestedFields.add(fieldName);// 当前类型set中放入当前字段名
                    }
                }
            }
            boolean loadSource = sourceRequired(context);
            if (storedToRequestedFields.isEmpty()) {
                // 大概就是无传入_source字段，啥都不返回
                // empty list specified, default to disable _source if no explicit indication
                return new FieldsVisitor(loadSource);
            } else {
                // 返回
                return new CustomFieldsVisitor(storedToRequestedFields.keySet(), loadSource);
            }
        }
    }

    private boolean sourceRequired(SearchContext context) {
        return context.sourceRequested() || context.fetchFieldsContext() != null;
    }

    private HitContext prepareHitContext(SearchContext context,
                                         LeafNestedDocuments nestedDocuments,
                                         Predicate<String> hasNonNestedParent,
                                         FieldsVisitor fieldsVisitor,
                                         int docId,
                                         Map<String, Set<String>> storedToRequestedFields,
                                         LeafReaderContext subReaderContext,
                                         CheckedBiConsumer<Integer, FieldsVisitor, IOException> storedFieldReader) throws IOException {
        // doc中无嵌套类型
        if (nestedDocuments.advance(docId - subReaderContext.docBase) == null) {
            return prepareNonNestedHitContext(
                context, fieldsVisitor, docId, storedToRequestedFields, subReaderContext, storedFieldReader);
        } else {
            return prepareNestedHitContext(context, docId, nestedDocuments, hasNonNestedParent, storedToRequestedFields,
                subReaderContext, storedFieldReader);
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source and setting it on {@link HitContext#sourceLookup()}. This
     *     allows fetch subphases that use the hit context to access the preloaded source.
     */
    private HitContext prepareNonNestedHitContext(SearchContext context,
                                                  FieldsVisitor fieldsVisitor,
                                                  int docId,
                                                  Map<String, Set<String>> storedToRequestedFields,
                                                  LeafReaderContext subReaderContext,
                                                  CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader) throws IOException {
        int subDocId = docId - subReaderContext.docBase;
        SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
        // 即无需返回字段
        if (fieldsVisitor == null) {
            // 全是空的
            SearchHit hit = new SearchHit(docId, null, new Text(searchExecutionContext.getType()), null, null);
            return new HitContext(hit, subReaderContext, subDocId);
        } else {
            SearchHit hit;
            // 通过reader加载数据到visitor
            /**
             * @see CodecReader#document(int, StoredFieldVisitor)
             * @see org.apache.lucene.codecs.simpletext.SimpleTextStoredFieldsReader#visitDocument(int, StoredFieldVisitor)
             */
            loadStoredFields(context.getSearchExecutionContext()::getFieldType, searchExecutionContext.getType(), fieldReader,
                fieldsVisitor, subDocId);

            // 字段_id
            Uid uid = fieldsVisitor.uid();
            if (fieldsVisitor.fields().isEmpty() == false) {
                Map<String, DocumentField> docFields = new HashMap<>();
                Map<String, DocumentField> metaFields = new HashMap<>();
                // 填充字段值
                fillDocAndMetaFields(context, fieldsVisitor, storedToRequestedFields, docFields, metaFields);
                // hit中 包含 全局doc id（用来查找segment、在segment中查找数据）、业务id（_id字段）、docFields（字段值）
                hit = new SearchHit(docId, uid.id(), new Text(searchExecutionContext.getType()), docFields, metaFields);
            } else {
                hit = new SearchHit(docId, uid.id(), new Text(searchExecutionContext.getType()), emptyMap(), emptyMap());
            }

            HitContext hitContext = new HitContext(hit, subReaderContext, subDocId);
            if (fieldsVisitor.source() != null) {
                // Store the loaded source on the hit context so that fetch subphases can access it.
                // Also make it available to scripts by storing it on the shared SearchLookup instance.
                hitContext.sourceLookup().setSource(fieldsVisitor.source());

                SourceLookup scriptSourceLookup = context.getSearchExecutionContext().lookup().source();
                scriptSourceLookup.setSegmentAndDocument(subReaderContext, subDocId);
                scriptSourceLookup.setSource(fieldsVisitor.source());
            }
            return hitContext;
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * nested document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source, filtering it based on the nested document ID, then
     *     setting it on {@link HitContext#sourceLookup()}. This allows fetch subphases that
     *     use the hit context to access the preloaded source.
     */
    @SuppressWarnings("unchecked")
    private HitContext prepareNestedHitContext(SearchContext context,
                                               int topDocId,
                                               LeafNestedDocuments nestedInfo,
                                               Predicate<String> hasNonNestedParent,
                                               Map<String, Set<String>> storedToRequestedFields,
                                               LeafReaderContext subReaderContext,
                                               CheckedBiConsumer<Integer, FieldsVisitor, IOException> storedFieldReader)
            throws IOException {
        // Also if highlighting is requested on nested documents we need to fetch the _source from the root document,
        // otherwise highlighting will attempt to fetch the _source from the nested doc, which will fail,
        // because the entire _source is only stored with the root document.
        boolean needSource = sourceRequired(context) || context.highlight() != null;

        Uid rootId;
        Map<String, Object> rootSourceAsMap = null;
        XContentType rootSourceContentType = null;

        SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
        if (context instanceof InnerHitsContext.InnerHitSubContext) {
            InnerHitsContext.InnerHitSubContext innerHitsContext = (InnerHitsContext.InnerHitSubContext) context;
            rootId = innerHitsContext.getRootId();

            if (needSource) {
                SourceLookup rootLookup = innerHitsContext.getRootLookup();
                rootSourceAsMap = rootLookup.source();
                rootSourceContentType = rootLookup.sourceContentType();
            }
        } else {
            FieldsVisitor rootFieldsVisitor = new FieldsVisitor(needSource);
            loadStoredFields(searchExecutionContext::getFieldType, searchExecutionContext.getType(),
                storedFieldReader, rootFieldsVisitor, nestedInfo.rootDoc());
            rootFieldsVisitor.postProcess(searchExecutionContext::getFieldType, searchExecutionContext.getType());
            rootId = rootFieldsVisitor.uid();

            if (needSource) {
                if (rootFieldsVisitor.source() != null) {
                    Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(rootFieldsVisitor.source(), false);
                    rootSourceAsMap = tuple.v2();
                    rootSourceContentType = tuple.v1();
                } else {
                    rootSourceAsMap = Collections.emptyMap();
                }
            }
        }

        Map<String, DocumentField> docFields = emptyMap();
        Map<String, DocumentField> metaFields = emptyMap();
        if (context.hasStoredFields() && context.storedFieldsContext().fieldNames().isEmpty() == false) {
            FieldsVisitor nestedFieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), false);
            loadStoredFields(searchExecutionContext::getFieldType, searchExecutionContext.getType(),
                storedFieldReader, nestedFieldsVisitor, nestedInfo.doc());
            if (nestedFieldsVisitor.fields().isEmpty() == false) {
                docFields = new HashMap<>();
                metaFields = new HashMap<>();
                fillDocAndMetaFields(context, nestedFieldsVisitor, storedToRequestedFields, docFields, metaFields);
            }
        }

        SearchHit.NestedIdentity nestedIdentity = nestedInfo.nestedIdentity();

        SearchHit hit = new SearchHit(topDocId, rootId.id(), new Text(searchExecutionContext.getType()),
            nestedIdentity, docFields, metaFields);
        HitContext hitContext = new HitContext(hit, subReaderContext, nestedInfo.doc());

        if (rootSourceAsMap != null && rootSourceAsMap.isEmpty() == false) {
            // Isolate the nested json array object that matches with nested hit and wrap it back into the same json
            // structure with the nested json array object being the actual content. The latter is important, so that
            // features like source filtering and highlighting work consistent regardless of whether the field points
            // to a json object array for consistency reasons on how we refer to fields
            Map<String, Object> nestedSourceAsMap = new HashMap<>();
            Map<String, Object> current = nestedSourceAsMap;
            for (SearchHit.NestedIdentity nested = nestedIdentity; nested != null; nested = nested.getChild()) {
                String nestedPath = nested.getField().string();
                current.put(nestedPath, new HashMap<>());
                List<?> nestedParsedSource = XContentMapValues.extractNestedValue(nestedPath, rootSourceAsMap);
                if ((nestedParsedSource.get(0) instanceof Map) == false && hasNonNestedParent.test(nestedPath)) {
                    // When one of the parent objects are not nested then XContentMapValues.extractValue(...) extracts the values
                    // from two or more layers resulting in a list of list being returned. This is because nestedPath
                    // encapsulates two or more object layers in the _source.
                    //
                    // This is why only the first element of nestedParsedSource needs to be checked.
                    throw new IllegalArgumentException("Cannot execute inner hits. One or more parent object fields of nested field [" +
                        nestedPath + "] are not nested. All parent fields need to be nested fields too");
                }
                rootSourceAsMap = (Map<String, Object>) nestedParsedSource.get(nested.getOffset());
                if (nested.getChild() == null) {
                    current.put(nestedPath, rootSourceAsMap);
                } else {
                    Map<String, Object> next = new HashMap<>();
                    current.put(nestedPath, next);
                    current = next;
                }
            }

            hitContext.sourceLookup().setSource(nestedSourceAsMap);
            hitContext.sourceLookup().setSourceContentType(rootSourceContentType);
        }
        return hitContext;
    }

    private void loadStoredFields(Function<String, MappedFieldType> fieldTypeLookup,
                                  @Nullable String type,
                                  CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader,
                                  FieldsVisitor fieldVisitor, int docId) throws IOException {
        fieldVisitor.reset();
        fieldReader.accept(docId, fieldVisitor);
        fieldVisitor.postProcess(fieldTypeLookup, type);
    }

    private static void fillDocAndMetaFields(SearchContext context, FieldsVisitor fieldsVisitor,
            Map<String, Set<String>> storedToRequestedFields,
            Map<String, DocumentField> docFields, Map<String, DocumentField> metaFields) {
        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
            String storedField = entry.getKey();
            List<Object> storedValues = entry.getValue();
            // 需要判断，是否包含这个类型的这个字段 （类型、名字限定，不然同名不同类型不可以返回结果）
            if (storedToRequestedFields.containsKey(storedField)) {
                for (String requestedField : storedToRequestedFields.get(storedField)) {
                    if (context.getSearchExecutionContext().isMetadataField(requestedField)) {
                        metaFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    } else {
                        // 放入字段名，DocumentField
                        docFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    }
                }
            } else {
                if (context.getSearchExecutionContext().isMetadataField(storedField)) {
                    metaFields.put(storedField, new DocumentField(storedField, storedValues));
                } else {
                    docFields.put(storedField, new DocumentField(storedField, storedValues));
                }
            }
        }
    }

    /**
     * Returns <code>true</code> if the provided <code>docs</code> are
     * stored sequentially (Dn = Dn-1 + 1).
     */
    static boolean hasSequentialDocs(DocIdToIndex[] docs) {
        return docs.length > 0 && docs[docs.length-1].docId - docs[0].docId == docs.length - 1;
    }
}
