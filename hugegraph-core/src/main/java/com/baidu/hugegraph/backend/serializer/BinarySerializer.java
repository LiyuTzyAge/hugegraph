/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.serializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.RangeConditions;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdPrefixQuery;
import com.baidu.hugegraph.backend.query.IdRangeQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry.BinaryId;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.AggregateType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.type.define.SerialEnum;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.NumericUtil;
import com.baidu.hugegraph.util.StringEncoding;

/**
 * 序列化过程：对象-》BytesBuffer-》BackendEntry -》send-》data backend write
 */
public class BinarySerializer extends AbstractSerializer {

    public static final byte[] EMPTY_BYTES = new byte[0];

    //?????? 是指id存储在rowKey中，天然索引中
    /*
     * Id is stored in column name if keyWithIdPrefix=true like RocksDB,
     * else stored in rowkey like HBase.
     */
    //Id前缀用于数据分布
    //(sys-prop,user-pro,edge-name) 是否带Id前缀
    private final boolean keyWithIdPrefix;
    // 是否带Id前缀 ，用于数据分布
    private final boolean indexWithIdPrefix;     //索引是否带Id前缀，用于数据分布

    public BinarySerializer() {
        this(true, true);
    }

    public BinarySerializer(boolean keyWithIdPrefix,
                            boolean indexWithIdPrefix) {
        this.keyWithIdPrefix = keyWithIdPrefix;
        this.indexWithIdPrefix = indexWithIdPrefix;
    }

    /**
     * id to BytesBuffer，to BinaryBackendEntry
     * 将id序列化为底层查询对象BinaryBackendEntry
     * @param type
     * @param id
     * @return
     */
    @Override
    public BinaryBackendEntry newBackendEntry(HugeType type, Id id) {
        //创建BytesBuffer，写入id
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        byte[] idBytes = type.isIndex() ?
                         buffer.writeIndexId(id, type).bytes() :
                         buffer.writeId(id).bytes();
        return new BinaryBackendEntry(type, new BinaryId(idBytes, id));
    }

    /**
     * HugeVertex 序列化
     * @param vertex
     * @return
     */
    protected final BinaryBackendEntry newBackendEntry(HugeVertex vertex) {
        return newBackendEntry(vertex.type(), vertex.id());
    }

    /**
     * EDGE序列化，edgeId with direction of BinaryId
     * owner-vertex + directory + edge-label + sort-values + other-vertex
     * @param edge
     * @return
     */
    protected final BinaryBackendEntry newBackendEntry(HugeEdge edge) {
        BinaryId id = new BinaryId(formatEdgeName(edge),
                                   edge.idWithDirection());
        return new BinaryBackendEntry(edge.type(), id);
    }

    /**
     * 元数据序列化
     * @param elem
     * @return
     */
    protected final BinaryBackendEntry newBackendEntry(SchemaElement elem) {
        return newBackendEntry(elem.type(), elem.id());
    }

    /**
     * convert to BinaryBackendEntry
     * @param entry
     * @return
     */
    @Override
    protected BinaryBackendEntry convertEntry(BackendEntry entry) {
        assert entry instanceof BinaryBackendEntry;
        return (BinaryBackendEntry) entry;
    }

    /**
     * Sysprop 系统属性名称序列化，不包含属性value
     * 场景1：keyWithIdPrefix 属性以Id作为前缀
     * 场景2：属性无前缀
     * @param id
     * @param col
     * @return
     */
    protected byte[] formatSyspropName(Id id, HugeKeys col) {
        //1=>id.head + id
        int idLen = this.keyWithIdPrefix ? 1 + id.length() : 0;
        //idLen+(1=sysprop)+(1=HugeKeys.col)
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 1 + 1);
        byte sysprop = HugeType.SYS_PROPERTY.code();
        if (this.keyWithIdPrefix) {
            buffer.writeId(id);
        }
        return buffer.write(sysprop).write(col.code()).bytes();
    }

    protected byte[] formatSyspropName(BinaryId id, HugeKeys col) {
        int idLen = this.keyWithIdPrefix ? id.length() : 0;
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 1 + 1);
        byte sysprop = HugeType.SYS_PROPERTY.code();
        if (this.keyWithIdPrefix) {
            buffer.write(id.asBytes());
        }
        return buffer.write(sysprop).write(col.code()).bytes();
    }

    /**
     * 序列化label id
     * @param elem
     * @return
     */
    protected BackendColumn formatLabel(HugeElement elem) {
        BackendColumn col = new BackendColumn();
        //序列化属性name；两种格式：id+label;label
        col.name = this.formatSyspropName(elem.id(), HugeKeys.LABEL);
        Id label = elem.schemaLabel().id();
        BytesBuffer buffer = BytesBuffer.allocate(label.length() + 1);
        //序列化属性id，因为属性唯一，所以id是单一数值或字符串，不包括vertex
        col.value = buffer.writeId(label).bytes();
        return col;
    }

    /**
     * 序列化属性name
     * elementId+prop.code+schemId
     * @param prop
     * @return
     */
    protected byte[] formatPropertyName(HugeProperty<?> prop) {
        Id id = prop.element().id();        //elementId
        int idLen = this.keyWithIdPrefix ? 1 + id.length() : 0;
        Id pkeyId = prop.propertyKey().id();    //schemId
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 2 + pkeyId.length());
        if (this.keyWithIdPrefix) {
            buffer.writeId(id);
        }
        buffer.write(prop.type().code());   //prop.code
        buffer.writeId(pkeyId); //schemId
        return buffer.bytes();
    }

    /**
     * 序列化property
     * name+bytes
     * @param prop
     * @return
     */
    protected BackendColumn formatProperty(HugeProperty<?> prop) {
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_PROPERTY);
        buffer.writeProperty(prop.propertyKey(), prop.value());     //序列化value
        //写入 propertyName 与 value
        return BackendColumn.of(this.formatPropertyName(prop), buffer.bytes());
    }

    /**
     * 反序列化属性
     * @param pkeyId
     * @param buffer
     * @param owner
     */
    protected void parseProperty(Id pkeyId, BytesBuffer buffer,
                                 HugeElement owner) {
        //get Property schema meta
        PropertyKey pkey = owner.graph().propertyKey(pkeyId);

        // Parse value from bytesBuffer
        Object value = buffer.readProperty(pkey);

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            //Set or List
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-single property: %s", value);
            }
            for (Object v : (Collection<?>) value) {
                owner.addProperty(pkey, v);
            }
        }
    }

    /**
     * 边 property
     * 批量序列化多个属性 与 formatproperty格式不同
     * size+[property_id+property_value]
     * @param props
     * @param buffer
     */
    protected void formatProperties(Collection<HugeProperty<?>> props,
                                    BytesBuffer buffer) {
        // Write properties size
        buffer.writeVInt(props.size());

        // Write properties data
        for (HugeProperty<?> property : props) {
            PropertyKey pkey = property.propertyKey();
            //property id
            buffer.writeVInt(SchemaElement.schemaId(pkey.id()));
            //property value
            buffer.writeProperty(pkey, property.value());
        }
    }

    /**
     * Edge/Vertex property
     * 反序列化 多个property
     * 结构 size+[property_id+property_value]
     * @param buffer
     * @param owner
     */
    protected void parseProperties(BytesBuffer buffer, HugeElement owner) {
        int size = buffer.readVInt();
        assert size >= 0;
        for (int i = 0; i < size; i++) {
            Id pkeyId = IdGenerator.of(buffer.readVInt());
            this.parseProperty(pkeyId, buffer, owner);
        }
    }

    /**
     * edge id 序列化,无Megic
     * @param edge
     * @return
     */
    protected byte[] formatEdgeName(HugeEdge edge) {
        // owner-vertex + directory + edge-label + sort-values + other-vertex
        return BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID)
                          .writeEdgeId(edge.id()).bytes();
    }

    /**
     * 序列化Edge properties
     * 多个property可以写在一起
     * @param edge
     * @return
     */
    protected byte[] formatEdgeValue(HugeEdge edge) {
        int propsCount = edge.getProperties().size();
        //size=4byte,id=4byte,value=12byte
        BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * propsCount);

        // Write edge id
        //buffer.writeId(edge.id());

        // Write edge properties
        this.formatProperties(edge.getProperties().values(), buffer);

        return buffer.bytes();
    }

    /**
     * 序列化Edge
     * @param edge
     * @return
     */
    protected BackendColumn formatEdge(HugeEdge edge) {
        byte[] name;
        if (this.keyWithIdPrefix) {
            name = this.formatEdgeName(edge);
        } else {
            name = EMPTY_BYTES;
        }
        return BackendColumn.of(name, this.formatEdgeValue(edge));
    }

    /**
     * 反序列化Edge，无properties
     * EdgeId 不包含 Megic
     * @param col 由发起点查询得到的edge
     * @param vertex 发起点、参照点
     * @param graph
     */
    protected void parseEdge(BackendColumn col, HugeVertex vertex,
                             HugeGraph graph) {
        // owner-vertex + dir + edge-label + sort-values + other-vertex

        BytesBuffer buffer = BytesBuffer.wrap(col.name);
        if (this.keyWithIdPrefix) {
            //BackendColumn.name 序列化EdgeId时无Megic
            // Consume owner-vertex id
            buffer.readId();    //owner
        }
        byte type = buffer.read();  //dir
        Id labelId = buffer.readId();   //label
        String sk = buffer.readStringWithEnding(); //sort
        Id otherVertexId = buffer.readId(); //other

        //创建Edge
        boolean isOutEdge = (type == HugeType.EDGE_OUT.code());
        EdgeLabel edgeLabel = graph.edgeLabel(labelId);
        //元数据中定义的src与target的label
        VertexLabel srcLabel = graph.vertexLabel(edgeLabel.sourceLabel());
        VertexLabel tgtLabel = graph.vertexLabel(edgeLabel.targetLabel());

        HugeVertex otherVertex;
        if (isOutEdge) {
            //vertex-->other ==> vertex=src , other=tgt
            vertex.vertexLabel(srcLabel);
            otherVertex = new HugeVertex(graph, otherVertexId, tgtLabel);
        } else {
            //vertex<--other ==> vertex=tgt , other=src
            vertex.vertexLabel(tgtLabel);
            otherVertex = new HugeVertex(graph, otherVertexId, srcLabel);
        }

        HugeEdge edge = new HugeEdge(graph, null, edgeLabel);
        edge.name(sk);
        edge.vertices(isOutEdge, vertex, otherVertex);
        edge.assignId();

        //Vertex cache edge，Edge cache vertex
        if (isOutEdge) {
            vertex.addOutEdge(edge);
            otherVertex.addInEdge(edge.switchOwner());
        } else {
            vertex.addInEdge(edge);
            otherVertex.addOutEdge(edge.switchOwner());
        }

        vertex.propNotLoaded();
        otherVertex.propNotLoaded();

        // Parse edge-id + edge-properties
        buffer = BytesBuffer.wrap(col.value);

        //Id id = buffer.readId();

        // Parse edge properties
        this.parseProperties(buffer, edge);
    }

    /**
     * 反序列化 column，每个column保存单个property与edge
     * type in (HugeType.PROPERTY.code()、HugeType.EDGE_IN.code()、HugeType
     * .EDGE_OUT.code()、HugeType.SYS_PROPERTY.code())
     * 根据type类型做对应的反序列化
     * @param col
     * @param vertex
     */
    protected void parseColumn(BackendColumn col, HugeVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(col.name);
        Id id = this.keyWithIdPrefix ? buffer.readId() : vertex.id();
        //如果keyWithIdPrefix=false，formatEdge时name=Empty，则type为空，buffer.remaining()
        //TODO:????
        E.checkState(buffer.remaining() > 0, "Missing column type");
        byte type = buffer.read();  //prop.code
        // Parse property
        if (type == HugeType.PROPERTY.code()) {
            Id pkeyId = buffer.readId();
            this.parseProperty(pkeyId, BytesBuffer.wrap(col.value), vertex);
        }
        // Parse edge
        else if (type == HugeType.EDGE_IN.code() ||
                 type == HugeType.EDGE_OUT.code()) {
            this.parseEdge(col, vertex, vertex.graph());
        }
        // Parse system property
        else if (type == HugeType.SYS_PROPERTY.code()) {
            // pass
        }
        // Invalid entry
        else {
            E.checkState(false, "Invalid entry(%s) with unknown type(%s): 0x%s",
                         id, type & 0xff, Bytes.toHex(col.name));
        }
    }

    /**
     * 序列化 HugeIndexName
     * 格式
     * indexWithIdPrefix =》index-id element-id (vertex/edge)-ID
     * ！indexWithIdPrefix =》element-id (vertex/edge)-ID
     * @param index
     * @return
     */
    protected byte[] formatIndexName(HugeIndex index) {
        BytesBuffer buffer;
        //IndexId用于数据分布，反序列化时无用
        if (!this.indexWithIdPrefix) {
            //only element-id ?? value写在哪？？
            Id elemId = index.elementId();      //elementId
            int idLen = 1 + elemId.length();
            buffer = BytesBuffer.allocate(idLen);
            // Write element-id
            buffer.writeId(elemId, true);
        } else {
            Id indexId = index.id();
            HugeType type = index.type();   //如 RANGE_INT_INDEX
            //String index
            if (!type.isNumericIndex() && indexIdLengthExceedLimit(indexId)) {
                indexId = index.hashId();       //hashIndexId
            }
            Id elemId = index.elementId();      //elementId
            //1 = Ending ; 1 = megic
            int idLen = 1 + elemId.length() + 1 + indexId.length();
            buffer = BytesBuffer.allocate(idLen);
            // Write index-id
            buffer.writeIndexId(indexId, type);
            // Write element-id
            buffer.writeId(elemId);     //index-id element-id
        }

        return buffer.bytes();
    }

    /**
     * 反序列化IndexName
     * Index->Index(elementId)
     * @param entry
     * @param index
     * @param fieldValues
     */
    protected void parseIndexName(BinaryBackendEntry entry, HugeIndex index,
                                  Object fieldValues) {
        for (BackendColumn col : entry.columns()) {
            //匹配索引值 默认使用字符串比较
            if (indexFieldValuesUnmatched(col.value, fieldValues)) {
                // Skip if field-values is not matched (just the same hash)
                continue;
            }
            BytesBuffer buffer = BytesBuffer.wrap(col.name);
            if (this.indexWithIdPrefix) {
                buffer.readIndexId(index.type());
            }
            index.elementIds(buffer.readId());
        }
    }

    /**
     * 序列化Vertex与properties
     * 格式：BackendEntry.col(name=vertex-id,value= labelId+size+properties)
     * @param vertex
     * @return
     */
    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        BinaryBackendEntry entry = newBackendEntry(vertex); //type vertex-id

        if (vertex.removed()) {
            return entry;
        }

        int propsCount = vertex.getProperties().size();
        //8=4+4,property=16
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);

        // Write vertex label
        buffer.writeId(vertex.schemaLabel().id());  //4bit

        //4bit+properties
        // Write all properties of the vertex
        this.formatProperties(vertex.getProperties().values(), buffer);

        //name=vertex-id value= labelId+size+properties
        entry.column(entry.id().asBytes(), buffer.bytes());

        return entry;
    }

    /**
     * 反序列化Vertex的元数据信息（label，properties）
     *  vertexlabel+size+[property_id+property_value]
     * @param value
     * @param vertex 当前vertex对象，无元数据信息
     */
    protected void parseVertex(byte[] value, HugeVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(value);

        // Parse vertex label
        VertexLabel label = vertex.graph().vertexLabel(buffer.readId());
        vertex.vertexLabel(label);

        // Parse properties
        this.parseProperties(buffer, vertex);
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        throw new NotImplementedException("Unsupported writeVertexProperty()");
    }

    /**
     * read vertex from BackendEntry which is the data of (vertex or edge).
     * Result is the edge or properties of the vertex.
     * @param graph
     * @param bytesEntry 可能是Edge或Vertex
     * @return
     */
    @Override
    public HugeVertex readVertex(HugeGraph graph, BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);

        // Parse id
        Id id = entry.id().origin();
        //ownerVertexId or vertexId
        Id vid = id.edge() ? ((EdgeId) id).ownerVertexId() : id;
        HugeVertex vertex = new HugeVertex(graph, vid, VertexLabel.NONE);

        // Parse all properties and edges of a Vertex
        for (BackendColumn col : entry.columns()) {
            if (entry.type().isEdge()) {
                //id=EdgeId;entry.type=Edge;col=edge 实际条件
                //col数据类型为edge，也传入ownerVertex用于存取数据，没有其他意义
                // NOTE: the entry id type is vertex even if entry type is edge
                // Parse vertex edges without properties
                this.parseColumn(col, vertex);
            } else {
                //数据与index读写是分开的，所以处理vertex else edge
                assert entry.type().isVertex();
                // Parse vertex properties
                assert entry.columnsSize() == 1 : entry.columnsSize();
                this.parseVertex(col.value, vertex);
            }
        }

        return vertex;
    }

    /**
     * 序列化 Edge ，BackendEntry(EdgeType,Edge.BinaryId)
     * BackendColumn 保存具体信息
     * @param edge
     * @return
     */
    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        BinaryBackendEntry entry = newBackendEntry(edge);
        entry.column(this.formatEdge(edge));
        return entry;
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        // TODO: entry.column(this.formatProperty(prop));
        throw new NotImplementedException("Unsupported writeEdgeProperty()");
    }

    @Override
    public HugeEdge readEdge(HugeGraph graph, BackendEntry bytesEntry) {
        throw new NotImplementedException("Unsupported readEdge()");
    }

    /**
     * 索引对象序列化
     * index.id + type -> BackendEntry.id+type
     * index.formatIndexName -> column.name
     *       -> column.value = null or value = index.fieldValues
     * 注：type 非 numberic，并且indexId超长，则进行Hash IndexId
     * @param index
     * @return
     */
    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        BinaryBackendEntry entry;
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            //当这个条件成立，说明根据indexLabel删除整个Index
            /*
             * When field-values is null and elementIds size is 0, it is
             * meaningful for deletion of index data by index label.
             * TODO: improve
             */
            entry = this.formatILDeletion(index);
        } else {
            Id id = index.id(); //BackendEntry id
            HugeType type = index.type(); //BackendEntry type
            byte[] value = null;
            if (!type.isNumericIndex() && indexIdLengthExceedLimit(id)) {
                id = index.hashId();
                // Save field-values as column value if the key is a hash string
                value = StringEncoding.encode(index.fieldValues().toString());
            }

            entry = newBackendEntry(type, id);
            //value=null else (!type.isNumericIndex() && indexIdLengthExceedLimit(id))
            entry.column(this.formatIndexName(index), value);
            entry.subId(index.elementId());
        }
        return entry;
    }

    /**
     * 根据查询反序列化Index
     * ConditionQuery+BackendEntry->Index(label,value,ElementId)
     * @param graph
     * @param query 索引查询条件
     * @param bytesEntry 索引查询结果
     * @return Index(label,value,ElementId)
     */
    @Override
    public HugeIndex readIndex(HugeGraph graph, ConditionQuery query,
                               BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }

        BinaryBackendEntry entry = this.convertEntry(bytesEntry);
        // NOTE: index id without length prefix
        byte[] bytes = entry.id().asBytes();    //indexId
        //Index(label,value)
        HugeIndex index = HugeIndex.parseIndexId(graph, entry.type(), bytes);

        Object fieldValues = null;
        //字符串index修正value-hash值
        if (!index.type().isRangeIndex()) {
            //查询条件中HugeKeys.FIELD_VALUES对应的值
            //即查询索引的条件值，字符串是等值匹配
            fieldValues = query.condition(HugeKeys.FIELD_VALUES);
            //value是hash值时，不相等
            if (!index.fieldValues().equals(fieldValues)) {
                // Update field-values for hashed index-id
                index.fieldValues(fieldValues); //替换掉hash值
            }
        }
        //Index(label,value)->Index(label,value,ElementId)
        this.parseIndexName(entry, index, fieldValues);
        return index;
    }

    /**
     * (type,id)->BackendEntry
     * @param type
     * @param id
     * @return
     */
    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        return newBackendEntry(type, id);
    }

    /**
     * id->EdgeId/normalId->BinaryId
     * BinaryId用于与底层交互
     * @param type
     * @param id
     * @return
     */
    @Override
    protected Id writeQueryId(HugeType type, Id id) {
        if (type.isEdge()) {
            id = writeEdgeId(id);   //BinaryId
        } else {
            BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
            id = new BinaryId(buffer.writeId(id).bytes(), id);
        }
        return id;
    }

    /**
     *Edge查询序列化
     * @param query
     * @return
     */
    @Override
    protected Query writeQueryEdgeCondition(Query query) {
        ConditionQuery cq = (ConditionQuery) query;
        if (cq.hasRangeCondition()) {
            //Edge range query
            return this.writeQueryEdgeRangeCondition(cq);
        } else {
            //Edge prefix query
            return this.writeQueryEdgePrefixCondition(cq);
        }
    }

    /**
     * Edge Range场景查询序列化
     * 将原始查询条件 cq，提取 EdgeRange
     * 必要区间条件（OWNER_VERTEX，DIRECTION,EDGElABEL,min，max等），
     * 将区间条件序列化构造startID与endID BinaryId，
     * 最后封装成：IdPrefixQuery（如果Max=null），IdRangeQuery 进行后续查询
     * @param cq
     * @return
     */
    private Query writeQueryEdgeRangeCondition(ConditionQuery cq) {
        //The SORT_VALUES Conditions
        List<Condition> sortValues = cq.syspropConditions(HugeKeys.SORT_VALUES);
        //[x,y]
        E.checkArgument(sortValues.size() >= 1 && sortValues.size() <= 2,
                        "Edge range query must be with sort-values range");
        // Would ignore target vertex
        Id vertex = cq.condition(HugeKeys.OWNER_VERTEX);    //ownerVertex
        Directions direction = cq.condition(HugeKeys.DIRECTION);    //dir
        if (direction == null) {
            direction = Directions.OUT; //default out
        }
        Id label = cq.condition(HugeKeys.LABEL);    //edge label
        //序列化查询条件，构造start与end BinaryID
        //ownerVertex+dir+labelId+(sortValue+otherVertex)
        int size = 1 + vertex.length() + 1 + label.length() + 16;
        BytesBuffer start = BytesBuffer.allocate(size);
        start.writeId(vertex);
        start.write(direction.type().code());
        start.writeId(label);

        BytesBuffer end = BytesBuffer.allocate(size);
        end.copyFrom(start);
        //start=end:ownerVertex+dir+labelId+

        RangeConditions range = new RangeConditions(sortValues);
        //[x,]
        if (range.keyMin() != null) {
            //将上界以UTF-8序列化
            start.writeStringRaw((String) range.keyMin());
        }
        //[,y]
        if (range.keyMax() != null) {
            //将下界以UTF-8序列化
            end.writeStringRaw((String) range.keyMax());
        }
        // Sort-value will be empty if there is no start sort-value
        // Empty: ownerVertex+dir+labelId+
        // Not Empty: ownerVertex+dir+labelId+sortValue
        Id startId = new BinaryId(start.bytes(), null);
        // Set endId as prefix if there is no end sort-value
        // Empty: ownerVertex+dir+labelId+
        // Not Empty: ownerVertex+dir+labelId+sortValue
        Id endId = new BinaryId(end.bytes(), null);

        boolean includeStart = range.keyMinEq();
        //分页
        if (cq.paging() && !cq.page().isEmpty()) {
            includeStart = true;
            byte[] position = PageState.fromString(cq.page()).position();
            E.checkArgument(Bytes.compare(position, startId.asBytes()) >= 0,
                            "Invalid page out of lower bound");
            startId = new BinaryId(position, null);
        }
        //无最大值
        if (range.keyMax() == null) {
            //TODO:使用前缀查询，如何实现数据排序，返回大于start的数据，底层数据就是有序的？
            return new IdPrefixQuery(cq, startId, includeStart, endId);
        }
        //有最大值，判断条件是否等于最大值
        return new IdRangeQuery(cq, startId, includeStart, endId,
                                range.keyMaxEq());
    }

    /**
     * Edge Query序列化
     * 根据cq 序列化条件EdgeId.KEYS，得到EdgeId.BinaryId，
     * 最后封装得到IdPrefixQuery
     * @param cq
     * @return
     */
    private Query writeQueryEdgePrefixCondition(ConditionQuery cq) {
        int count = 0;
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
        //构建EdgeId查询前缀
        //min:ownerVertex+dir
        //max:ownerVertex+dir+labelId+(sortValue+StrEnding)+otherVertex
        for (HugeKeys key : EdgeId.KEYS) {
            Object value = cq.condition(key);

            if (value != null) {
                count++;
            } else {
                if (key == HugeKeys.DIRECTION) {
                    // Direction is null, set to OUT
                    value = Directions.OUT;
                } else {
                    //ownerVertex+dir+以上即可退出
                    //ownerVertex != null
                    break;
                }
            }

            if (key == HugeKeys.OWNER_VERTEX ||
                key == HugeKeys.OTHER_VERTEX) {
                buffer.writeId((Id) value);
            } else if (key == HugeKeys.DIRECTION) {
                byte t = ((Directions) value).type().code();
                buffer.write(t);
            } else if (key == HugeKeys.LABEL) {
                assert value instanceof Id;
                buffer.writeId((Id) value);
            } else if (key == HugeKeys.SORT_VALUES) {
                assert value instanceof String;
                //storeValue 以string写入，需要Ending确定数据字节长度
                buffer.writeStringWithEnding((String) value);
            } else {
                assert false : key;
            }
        }

        if (count > 0) {
            assert count == cq.conditions().size();
            return prefixQuery(cq, new BinaryId(buffer.bytes(), null));
        }

        return null;
    }

    /**
     * 序列化索引查询
     * RangeConditionQuery->RangeIndexIdQuery
     * String/TextConditionQuery->prefixQuery
     * @param query
     * @return
     */
    @Override
    protected Query writeQueryCondition(Query query) {
        HugeType type = query.resultType();
        if (!type.isIndex()) {
            return query;
        }
        //Query.type isIndex
        ConditionQuery cq = (ConditionQuery) query;
        //Convert index query to index id query
        if (type.isNumericIndex()) {
            //索引区间查询-》indexId区间查询
            // Convert range-index/shard-index query to indexId range query
            return this.writeRangeIndexQuery(cq);
        } else {
            assert type.isSearchIndex() || type.isSecondaryIndex() ||
                   type.isUniqueIndex();
            //索引等值匹配或模糊匹配（二级索引查询）-》indexId前缀查询
            // Convert secondary-index or search-index query to id query
            return this.writeStringIndexQuery(cq);
        }
    }

    /**
     * 字符串等值或模糊匹配查询，创建前缀匹配查询prefixQuery
     * ConditionQuery->(IndexLabel,value)->IndexId->prefixQuery
     * @param query
     * @return
     */
    private Query writeStringIndexQuery(ConditionQuery query) {
        E.checkArgument(query.allSysprop() &&
                        query.conditions().size() == 2,
                        "There should be two conditions: " +
                        "INDEX_LABEL_ID and FIELD_VALUES" +
                        "in secondary index query");

        Id index = query.condition(HugeKeys.INDEX_LABEL_ID);
        Object key = query.condition(HugeKeys.FIELD_VALUES);

        E.checkArgument(index != null, "Please specify the index label");
        E.checkArgument(key != null, "Please specify the index key");

        Id prefix = formatIndexId(query.resultType(), index, key, true);
        return prefixQuery(query, prefix);
    }

    /**
     * 索引范围查询,根据condition-》indexId
     * 根据查询条件中的fields，创建RangeConditions 区间条件对象，
     * 根据区间条件对象的等值，最大值，最小值等情况，创建条件的IndexId-min/max。
     * 最后创建最后创建IdRangeQuery(query, start, keyMinEq, max, keyMaxEq)
     * ConditionQuery->FIELD_VALUES->RangeConditions->min/max/eq->start-indexId/max-indexId->IdRangeQuery
     * @param query
     * @return
     */
    private Query writeRangeIndexQuery(ConditionQuery query) {
        Id index = query.condition(HugeKeys.INDEX_LABEL_ID);    //index label id
        E.checkArgument(index != null, "Please specify the index label");
        //FIELD_VALUES 查询条件 [x,y]
        List<Condition> fields = query.syspropConditions(HugeKeys.FIELD_VALUES);
        E.checkArgument(!fields.isEmpty(),
                        "Please specify the index field values");

        HugeType type = query.resultType(); //type
        Id start = null;
        if (query.paging() && !query.page().isEmpty()) {
            byte[] position = PageState.fromString(query.page()).position();
            start = new BinaryId(position, null);
        }

        RangeConditions range = new RangeConditions(fields);
        //等值匹配
        if (range.keyEq() != null) {
            //indexId
            Id id = formatIndexId(type, index, range.keyEq(), true);
            if (start == null) {
                //等值匹配直接返回IdPrefixQuery(indexId)
                return new IdPrefixQuery(query, id);
            }
            E.checkArgument(Bytes.compare(start.asBytes(), id.asBytes()) >= 0,
                            "Invalid page out of lower bound");
            //等值匹配分页查询
            return new IdPrefixQuery(query, start, id);
        }

        Object keyMin = range.keyMin();
        Object keyMax = range.keyMax();
        boolean keyMinEq = range.keyMinEq();
        boolean keyMaxEq = range.keyMaxEq();
        if (keyMin == null) {
            E.checkArgument(keyMax != null,
                            "Please specify at least one condition");
            //根据数据类型返回最小值
            // Set keyMin to min value
            keyMin = NumericUtil.minValueOf(keyMax.getClass());
            keyMinEq = true;
        }
        //indexId-minvalue
        Id min = formatIndexId(type, index, keyMin, false);
        if (!keyMinEq) {    //不包含最小值
            /*
             * Increase 1 to keyMin, index GT query is a scan with GT prefix,
             * inclusiveStart=false will also match index started with keyMin
             */
            increaseOne(min.asBytes()); //根据最小单位加1
            keyMinEq = true;
        }

        if (start == null) {
            start = min;
        } else {
            E.checkArgument(Bytes.compare(start.asBytes(), min.asBytes()) >= 0,
                            "Invalid page out of lower bound");
        }

        if (keyMax == null) {
            keyMax = NumericUtil.maxValueOf(keyMin.getClass());
            keyMaxEq = true;
        }
        //indexId=maxValue
        Id max = formatIndexId(type, index, keyMax, false);
        if (keyMaxEq) {
            keyMaxEq = false;
            increaseOne(max.asBytes());
        }
        //keyMinEq=true,keyMaxEq=false,start=min/min=1，max=max/max+1
        // IdRangeQuery ==> [start,end)
        return new IdRangeQuery(query, start, keyMinEq, max, keyMaxEq);
    }

    /**
     * format index label deletion
     * 删除整个index，删除label
     * BackendEntry(indexType,labelId)
     * BackendColumn(labelId,null)
     * @param index
     * @return
     */
    private BinaryBackendEntry formatILDeletion(HugeIndex index) {
        Id id = index.indexLabelId();
        BinaryId bid = new BinaryId(id.asBytes(), id);
        //labelId
        BinaryBackendEntry entry = new BinaryBackendEntry(index.type(), bid);
        if (index.type().isStringIndex()) {
            //labelId
            byte[] idBytes = IdGenerator.of(id.asString()).asBytes();
            BytesBuffer buffer = BytesBuffer.allocate(idBytes.length);
            buffer.write(idBytes);
            //labelId
            entry.column(buffer.bytes(), null);
        } else {
            assert index.type().isRangeIndex();
            BytesBuffer buffer = BytesBuffer.allocate(4);
            //labelId
            buffer.writeInt((int) id.asLong());
            entry.column(buffer.bytes(), null);
        }
        return entry;
    }

    /**
     * id->EdgeId->BinaryId
     * @param id
     * @return
     */
    private static BinaryId writeEdgeId(Id id) {
        EdgeId edgeId;
        if (id instanceof EdgeId) {
            edgeId = (EdgeId) id;
        } else {
            edgeId = EdgeId.parse(id.asString());
        }
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID)
                                        .writeEdgeId(edgeId);
        return new BinaryId(buffer.bytes(), id);
    }

    /**
     * 创建前缀匹配查询
     * @param query
     * @param prefix
     * @return
     */
    private static Query prefixQuery(ConditionQuery query, Id prefix) {
        Query newQuery;
        if (query.paging() && !query.page().isEmpty()) {
            /*
             * If used paging and the page number is not empty, deserialize
             * the page to id and use it as the starting row for this query
             */
            byte[] position = PageState.fromString(query.page()).position();
            E.checkArgument(Bytes.compare(position, prefix.asBytes()) >= 0,
                            "Invalid page out of lower bound");
            BinaryId start = new BinaryId(position, null);
            newQuery = new IdPrefixQuery(query, start, prefix);
        } else {
            newQuery = new IdPrefixQuery(query, prefix);
        }
        return newQuery;
    }

    /**
     * 根据查询条件fieldValues，创建IndexId
     * 如果是等值匹配或Range则写入Ending，结束IndexId
     * @param type
     * @param indexLabel
     * @param fieldValues 查询条件
     * @param equal 是否等值匹配
     * @return
     */
    protected static BinaryId formatIndexId(HugeType type, Id indexLabel,
                                            Object fieldValues,
                                            boolean equal) {
        boolean withEnding = type.isRangeIndex() || equal;
        //indexId
        Id id = HugeIndex.formatIndexId(type, indexLabel, fieldValues);
        if (!type.isNumericIndex() && indexIdLengthExceedLimit(id)) {
            //hash
            id = HugeIndex.formatIndexHashId(type, indexLabel, fieldValues);
        }
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        //indexId如果是string则写入stringEnding
        byte[] idBytes = buffer.writeIndexId(id, type, withEnding).bytes();
        return new BinaryId(idBytes, id);
    }

    protected static boolean indexIdLengthExceedLimit(Id id) {
        return id.asBytes().length > BytesBuffer.INDEX_HASH_ID_THRESHOLD;
    }

    /**
     * 字符串比较
     * @param value
     * @param fieldValues
     * @return
     */
    protected static boolean indexFieldValuesUnmatched(byte[] value,
                                                       Object fieldValues) {
        if (value != null && value.length > 0 && fieldValues != null) {
            if (!StringEncoding.decode(value).equals(fieldValues)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 字节数组增长最小1bit
     * 如果最后一个byte 是最大值，则向前进位9like [1, 255] => [2, 0])
     * @param bytes
     * @return
     */
    public static final byte[] increaseOne(byte[] bytes) {
        final byte BYTE_MAX_VALUE = (byte) 0xff;
        assert bytes.length > 0;
        byte last = bytes[bytes.length - 1];
        if (last != BYTE_MAX_VALUE) {
            bytes[bytes.length - 1] += 0x01;    //最后1位加1
        } else {
            // Process overflow (like [1, 255] => [2, 0])
            int i = bytes.length - 1;
            //进位
            for (; i > 0 && bytes[i] == BYTE_MAX_VALUE; --i) {
                bytes[i] += 0x01;
            }
            if (bytes[i] == BYTE_MAX_VALUE) {
                assert i == 0;
                throw new BackendException("Unable to increase bytes: %s",
                                           Bytes.toHex(bytes));
            }
            bytes[i] += 0x01;
        }
        return bytes;
    }

    /**
     * vertexLabel 序列化 BackendEntry(Vertexlabel)
     * @param vertexLabel
     * @return
     */
    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeVertexLabel(vertexLabel);
    }

    /**
     * vertexLabel 反序列化
     * @param graph
     * @param backendEntry
     * @return
     */
    @Override
    public VertexLabel readVertexLabel(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readVertexLabel(graph, entry);
    }

    /**
     * EdgeLabel 序列化/反序列化
     * @param edgeLabel
     * @return
     */
    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeEdgeLabel(edgeLabel);
    }

    @Override
    public EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readEdgeLabel(graph, entry);
    }

    /**
     * PropertyKey 序列化
     * @param propertyKey
     * @return
     */
    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writePropertyKey(propertyKey);
    }

    @Override
    public PropertyKey readPropertyKey(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readPropertyKey(graph, entry);
    }

    /**
     * IndexLabel 序列化/反序列化
     * @param indexLabel
     * @return
     */
    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeIndexLabel(indexLabel);
    }

    @Override
    public IndexLabel readIndexLabel(HugeGraph graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readIndexLabel(graph, entry);
    }

    /**
     * schema 序列化工具类，嵌入式（嵌入原对象，简化操作）
     * 格式：
     * BackendEntry(Schema.type,Schema.id)
     * BackendColumn[name=(Megic+SchemId)+Prop.code,value=Ids.num(2bit)+
     * (Megic+id)*n]
     */
    private final class SchemaSerializer {

        private BinaryBackendEntry entry;

        /**
         * 序列化VertexLabel，序列化所有属性
         * @param schema
         * @return
         */
        public BinaryBackendEntry writeVertexLabel(VertexLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.ID_STRATEGY, schema.idStrategy());
            writeIds(HugeKeys.PROPERTIES, schema.properties());
            writeIds(HugeKeys.PRIMARY_KEYS, schema.primaryKeys());
            writeIds(HugeKeys.NULLABLE_KEYS, schema.nullableKeys());
            writeIds(HugeKeys.INDEX_LABELS, schema.indexLabels());
            writeBool(HugeKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        /**
         * vertexLabel 反序列化
         * @param graph
         * @param entry
         * @return
         */
        public VertexLabel readVertexLabel(HugeGraph graph,
                                           BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            VertexLabel vertexLabel = new VertexLabel(graph, id, name);
            vertexLabel.idStrategy(readEnum(HugeKeys.ID_STRATEGY,
                                            IdStrategy.class));
            vertexLabel.properties(readIds(HugeKeys.PROPERTIES));
            vertexLabel.primaryKeys(readIds(HugeKeys.PRIMARY_KEYS));
            vertexLabel.nullableKeys(readIds(HugeKeys.NULLABLE_KEYS));
            vertexLabel.indexLabels(readIds(HugeKeys.INDEX_LABELS));
            vertexLabel.enableLabelIndex(readBool(HugeKeys.ENABLE_LABEL_INDEX));
            vertexLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(vertexLabel);
            return vertexLabel;
        }

        /**
         * EdgeLabel 序列化
         * @param schema
         * @return
         */
        public BinaryBackendEntry writeEdgeLabel(EdgeLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeId(HugeKeys.SOURCE_LABEL, schema.sourceLabel());
            writeId(HugeKeys.TARGET_LABEL, schema.targetLabel());
            writeEnum(HugeKeys.FREQUENCY, schema.frequency());
            writeIds(HugeKeys.PROPERTIES, schema.properties());
            writeIds(HugeKeys.SORT_KEYS, schema.sortKeys());
            writeIds(HugeKeys.NULLABLE_KEYS, schema.nullableKeys());
            writeIds(HugeKeys.INDEX_LABELS, schema.indexLabels());
            writeBool(HugeKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        /**
         * EdgeLabel 反序列化
         * @param graph
         * @param entry
         * @return
         */
        public EdgeLabel readEdgeLabel(HugeGraph graph,
                                       BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            EdgeLabel edgeLabel = new EdgeLabel(graph, id, name);
            edgeLabel.sourceLabel(readId(HugeKeys.SOURCE_LABEL));
            edgeLabel.targetLabel(readId(HugeKeys.TARGET_LABEL));
            edgeLabel.frequency(readEnum(HugeKeys.FREQUENCY, Frequency.class));
            edgeLabel.properties(readIds(HugeKeys.PROPERTIES));
            edgeLabel.sortKeys(readIds(HugeKeys.SORT_KEYS));
            edgeLabel.nullableKeys(readIds(HugeKeys.NULLABLE_KEYS));
            edgeLabel.indexLabels(readIds(HugeKeys.INDEX_LABELS));
            edgeLabel.enableLabelIndex(readBool(HugeKeys.ENABLE_LABEL_INDEX));
            edgeLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(edgeLabel);
            return edgeLabel;
        }

        /**
         * ProperyKey 序列化/反序列化
         * @param schema
         * @return
         */
        public BinaryBackendEntry writePropertyKey(PropertyKey schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.DATA_TYPE, schema.dataType());
            writeEnum(HugeKeys.CARDINALITY, schema.cardinality());
            writeEnum(HugeKeys.AGGREGATE_TYPE, schema.aggregateType());
            writeIds(HugeKeys.PROPERTIES, schema.properties()); //empty
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public PropertyKey readPropertyKey(HugeGraph graph,
                                           BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            PropertyKey propertyKey = new PropertyKey(graph, id, name);
            propertyKey.dataType(readEnum(HugeKeys.DATA_TYPE, DataType.class));
            propertyKey.cardinality(readEnum(HugeKeys.CARDINALITY,
                                             Cardinality.class));
            propertyKey.aggregateType(readEnum(HugeKeys.AGGREGATE_TYPE,
                                               AggregateType.class));
            propertyKey.properties(readIds(HugeKeys.PROPERTIES));
            propertyKey.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(propertyKey);
            return propertyKey;
        }

        /**
         * indexlabel 序列化/反序列化
         * @param schema
         * @return
         */
        public BinaryBackendEntry writeIndexLabel(IndexLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.BASE_TYPE, schema.baseType());
            writeId(HugeKeys.BASE_VALUE, schema.baseValue());
            writeEnum(HugeKeys.INDEX_TYPE, schema.indexType());
            writeIds(HugeKeys.FIELDS, schema.indexFields());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public IndexLabel readIndexLabel(HugeGraph graph,
                                         BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            IndexLabel indexLabel = new IndexLabel(graph, id, name);
            indexLabel.baseType(readEnum(HugeKeys.BASE_TYPE, HugeType.class));
            indexLabel.baseValue(readId(HugeKeys.BASE_VALUE));
            indexLabel.indexType(readEnum(HugeKeys.INDEX_TYPE,
                                          IndexType.class));
            indexLabel.indexFields(readIds(HugeKeys.FIELDS));
            indexLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(indexLabel);
            return indexLabel;
        }

        /**
         * UserData 序列化 ：map->json str -> UTF-8
         * @param schema
         */
        private void writeUserdata(SchemaElement schema) {
            //map->json str -> UTF-8
            String userdataStr = JsonUtil.toJson(schema.userdata());
            writeString(HugeKeys.USER_DATA, userdataStr);
        }

        private void readUserdata(SchemaElement schema) {
            // Parse all user data of a schema element
            byte[] userdataBytes = column(HugeKeys.USER_DATA);
            String userdataStr = StringEncoding.decode(userdataBytes);
            @SuppressWarnings("unchecked")
            Map<String, Object> userdata = JsonUtil.fromJson(userdataStr,
                                                             Map.class);
            for (Map.Entry<String, Object> e : userdata.entrySet()) {
                schema.userdata(e.getKey(), e.getValue());
            }
        }

        /**
         * 写入字符串属性 UTF-8
         * @param key
         * @param value
         */
        private void writeString(HugeKeys key, String value) {
            this.entry.column(formatColumnName(key),
                              StringEncoding.encode(value));
        }

        private String readString(HugeKeys key) {
            return StringEncoding.decode(column(key));
        }

        /**
         * 写入Enum ,value=Enum.code
         * @param key
         * @param value
         */
        private void writeEnum(HugeKeys key, SerialEnum value) {
            this.entry.column(formatColumnName(key), new byte[]{value.code()});
        }

        private <T extends SerialEnum> T readEnum(HugeKeys key,
                                                  Class<T> clazz) {
            byte[] value = column(key);
            E.checkState(value.length == 1,
                         "The length of column '%s' must be 1, but is '%s'",
                         key, value.length);
            return SerialEnum.fromCode(clazz, value[0]);
        }

        private void writeId(HugeKeys key, Id value) {
            this.entry.column(formatColumnName(key), writeId(value));
        }

        private Id readId(HugeKeys key) {
            return readId(column(key));
        }

        private void writeIds(HugeKeys key, Collection<Id> value) {
            this.entry.column(formatColumnName(key), writeIds(value));
        }

        private Id[] readIds(HugeKeys key) {
            return readIds(column(key));
        }

        private void writeBool(HugeKeys key, boolean value) {
            this.entry.column(formatColumnName(key),
                              new byte[]{(byte) (value ? 1 : 0)});
        }

        private boolean readBool(HugeKeys key) {
            byte[] value = column(key);
            E.checkState(value.length == 1,
                         "The length of column '%s' must be 1, but is '%s'",
                         key, value.length);
            return value[0] != (byte) 0;
        }

        private byte[] writeId(Id id) {
            int size = 1 + id.length();
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeId(id);
            return buffer.bytes();
        }

        private Id readId(byte[] value) {
            BytesBuffer buffer = BytesBuffer.wrap(value);
            return buffer.readId();
        }

        /**
         * schema.value包含多个Id，序列化格式：
         * Ids.num(2bit)+(Megic+id)*n
         * @param ids
         * @return
         */
        private byte[] writeIds(Collection<Id> ids) {
            E.checkState(ids.size() <= BytesBuffer.UINT16_MAX,
                         "The number of properties of vertex/edge label " +
                         "can't exceed '%s'", BytesBuffer.UINT16_MAX);
            int size = 2;
            for (Id id : ids) {
                size += (1 + id.length());
            }
            //size = 2+(1+id.length)*n  总长度
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeUInt16(ids.size()); //2bit id.size
            for (Id id : ids) {
                buffer.writeId(id); //Megic+id
            }
            return buffer.bytes();
        }

        private Id[] readIds(byte[] value) {
            BytesBuffer buffer = BytesBuffer.wrap(value);
            int size = buffer.readUInt16();
            Id[] ids = new Id[size];
            for (int i = 0; i < size; i++) {
                Id id = buffer.readId();
                ids[i] = id;
            }
            return ids;
        }

        /**
         * 返回Key对应的value
         * @param key
         * @return
         */
        private byte[] column(HugeKeys key) {
            BackendColumn column = this.entry.column(formatColumnName(key));
            E.checkState(column != null, "Not found key '%s' from entry %s",
                         key, this.entry);
            E.checkNotNull(column.value, "column.value");
            return column.value;
        }

        /**
         * 将Schema的属性Key序列化化为BackendColumn.name的格式
         * 格式=>(Megic+SchemaId(LongId))+HugeKeys.code
         * @param key
         * @return
         */
        private byte[] formatColumnName(HugeKeys key) {
            Id id = this.entry.id().origin();
            //1 + id.length()=Megic+id
            //1 = code
            int size = 1 + id.length() + 1;
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeId(id);
            buffer.write(key.code());
            return buffer.bytes();
        }
    }
}
