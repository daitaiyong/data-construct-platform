<script setup>
import { computed, ref } from 'vue'

const apiBase = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8080'
const apiStatus = ref('未检查')
const stage = ref(null)
const selectedNodeID = ref('')
const sourceNodeID = ref('')
const targetNodeID = ref('')
const validationMessage = ref('')
const validationError = ref(false)
const mappingError = ref('')

const dag = ref({
  nodes: [
    {
      node_id: 'node-001',
      node_name: '生成用户',
      template_id: 'tpl_user_001',
      position: { x: 48, y: 70 },
      config: { batch_size: 100, var_export: ['user_id_list'], fail_strategy: 'abort_dag' },
    },
    {
      node_id: 'node-002',
      node_name: '生成商品',
      template_id: 'tpl_goods_001',
      position: { x: 48, y: 300 },
      config: { batch_size: 50, var_export: ['goods_id_list'], fail_strategy: 'continue' },
    },
    {
      node_id: 'node-003',
      node_name: '生成订单',
      template_id: 'tpl_order_001',
      position: { x: 420, y: 180 },
      config: {
        batch_size: 200,
        var_mapping: {
          user_id: '${node-001.user_id_list}',
          goods_id: '${node-002.goods_id_list}',
        },
        fail_strategy: 'abort_dag',
      },
    },
  ],
  edges: [
    { edge_id: 'edge-01', source_node_id: 'node-001', target_node_id: 'node-003', label: '用户依赖' },
    { edge_id: 'edge-02', source_node_id: 'node-002', target_node_id: 'node-003', label: '商品依赖' },
  ],
  dag_meta: { version: 'v1', parallel: true },
})

const selectedNode = computed(() => dag.value.nodes.find((node) => node.node_id === selectedNodeID.value))
const dagContent = computed(() => JSON.stringify(dag.value, null, 2))
const exportText = computed({
  get: () => (selectedNode.value?.config.var_export || []).join(', '),
  set: (value) => {
    if (selectedNode.value) {
      selectedNode.value.config.var_export = value.split(',').map((item) => item.trim()).filter(Boolean)
    }
  },
})
const mappingText = computed({
  get: () => JSON.stringify(selectedNode.value?.config.var_mapping || {}, null, 2),
  set: (value) => {
    if (!selectedNode.value) return
    try {
      const mapping = JSON.parse(value || '{}')
      if (mapping === null || Array.isArray(mapping) || typeof mapping !== 'object') {
        throw new Error('映射必须是 JSON 对象')
      }
      selectedNode.value.config.var_mapping = mapping
      mappingError.value = ''
    } catch (error) {
      mappingError.value = error.message
    }
  },
})
const edgeLines = computed(() => dag.value.edges.map((edge) => {
  const source = dag.value.nodes.find((node) => node.node_id === edge.source_node_id)
  const target = dag.value.nodes.find((node) => node.node_id === edge.target_node_id)
  if (!source || !target) return null
  return {
    ...edge,
    x1: source.position.x + 190,
    y1: source.position.y + 54,
    x2: target.position.x,
    y2: target.position.y + 54,
  }
}).filter(Boolean))

let dragState = null

async function checkAPI() {
  apiStatus.value = '检查中…'
  try {
    const response = await fetch(`${apiBase}/healthz`)
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const result = await response.json()
    apiStatus.value = result.status === 'ok' ? '在线' : '异常'
  } catch (error) {
    apiStatus.value = `不可用：${error.message}`
  }
}

function addNode() {
  const id = `node-${Date.now()}`
  dag.value.nodes.push({
    node_id: id,
    node_name: '新节点',
    template_id: '',
    position: { x: 340, y: 70 + (dag.value.nodes.length % 3) * 145 },
    config: { batch_size: 100, var_export: [], var_mapping: {}, fail_strategy: 'abort_dag' },
  })
  selectedNodeID.value = id
}

function removeNode(nodeID) {
  dag.value.nodes = dag.value.nodes.filter((node) => node.node_id !== nodeID)
  dag.value.edges = dag.value.edges.filter((edge) => edge.source_node_id !== nodeID && edge.target_node_id !== nodeID)
  if (selectedNodeID.value === nodeID) selectedNodeID.value = ''
}

function connectNodes() {
  if (!sourceNodeID.value || !targetNodeID.value || sourceNodeID.value === targetNodeID.value) return
  const duplicate = dag.value.edges.some((edge) =>
    edge.source_node_id === sourceNodeID.value && edge.target_node_id === targetNodeID.value)
  if (duplicate) {
    validationError.value = true
    validationMessage.value = '这两个节点之间的连线已存在。'
    return
  }
  dag.value.edges.push({
    edge_id: `edge-${Date.now()}`,
    source_node_id: sourceNodeID.value,
    target_node_id: targetNodeID.value,
    label: '',
  })
  validationMessage.value = ''
}

function startDrag(event, node) {
  if (event.button !== 0 || event.target.closest('button')) return
  const bounds = stage.value.getBoundingClientRect()
  dragState = {
    node,
    offsetX: event.clientX - bounds.left - node.position.x,
    offsetY: event.clientY - bounds.top - node.position.y,
  }
  event.currentTarget.setPointerCapture(event.pointerId)
}

function moveNode(event) {
  if (!dragState) return
  const bounds = stage.value.getBoundingClientRect()
  dragState.node.position.x = Math.max(0, Math.round(event.clientX - bounds.left - dragState.offsetX))
  dragState.node.position.y = Math.max(0, Math.round(event.clientY - bounds.top - dragState.offsetY))
}

function stopDrag() {
  dragState = null
}

async function validateDAG() {
  validationMessage.value = '正在校验…'
  validationError.value = false
  try {
    const response = await fetch(`${apiBase}/api/v1/dags/validate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: dagContent.value,
    })
    const result = await response.json()
    if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`)
    validationMessage.value = `校验通过，拓扑顺序：${result.order.join(' → ')}`
  } catch (error) {
    validationError.value = true
    validationMessage.value = `校验失败：${error.message}`
  }
}
</script>

<template>
  <main class="page">
    <header class="page-header">
      <div>
        <p class="eyebrow">DATA CONSTRUCT PLATFORM</p>
        <h1>DAG 编排画布</h1>
        <p class="intro">拖动节点调整布局，配置依赖关系并校验拓扑。画布坐标只用于渲染，不参与执行排序。</p>
      </div>
      <button class="secondary-button" type="button" @click="checkAPI">API：{{ apiStatus }}</button>
    </header>

    <section class="workspace">
      <div class="canvas-column">
        <div class="toolbar">
          <strong>编排画布</strong>
          <div class="toolbar-actions">
            <button class="secondary-button" type="button" @click="addNode">添加节点</button>
            <button type="button" @click="validateDAG">校验 DAG</button>
          </div>
        </div>

        <div class="canvas-scroll">
          <div
            ref="stage"
            class="canvas-stage"
            @pointermove="moveNode"
            @pointerup="stopDrag"
            @pointercancel="stopDrag"
          >
            <svg class="edge-layer" aria-hidden="true">
              <defs>
                <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
                  <path d="M0,0 L0,6 L9,3 z" fill="#94a3b8" />
                </marker>
              </defs>
              <line
                v-for="edge in edgeLines"
                :key="edge.edge_id"
                :x1="edge.x1"
                :y1="edge.y1"
                :x2="edge.x2"
                :y2="edge.y2"
                marker-end="url(#arrow)"
              />
            </svg>
            <article
              v-for="node in dag.nodes"
              :key="node.node_id"
              class="dag-node"
              :class="{ selected: selectedNodeID === node.node_id }"
              :style="{ left: `${node.position.x}px`, top: `${node.position.y}px` }"
              @pointerdown="startDrag($event, node)"
              @click="selectedNodeID = node.node_id"
            >
              <div class="node-heading">
                <span class="node-dot" />
                <strong>{{ node.node_name }}</strong>
                <button class="icon-button" type="button" aria-label="删除节点" @click.stop="removeNode(node.node_id)">×</button>
              </div>
              <span class="node-id">{{ node.node_id }}</span>
              <span class="node-template">模板：{{ node.template_id || '未设置' }}</span>
              <span class="node-batch">批次：{{ node.config.batch_size }}</span>
            </article>
          </div>
        </div>

        <p v-if="validationMessage" class="validation-message" :class="{ error: validationError }">
          {{ validationMessage }}
        </p>
      </div>

      <aside class="side-column">
        <section class="panel">
          <h2>节点配置</h2>
          <p v-if="!selectedNode" class="muted">选择画布上的节点以编辑配置。</p>
          <form v-else class="node-form" @submit.prevent>
            <label>
              节点名称
              <input v-model="selectedNode.node_name" required />
            </label>
            <label>
              模板 ID
              <input v-model="selectedNode.template_id" required />
            </label>
            <label>
              批次大小
              <input v-model.number="selectedNode.config.batch_size" type="number" min="1" />
            </label>
            <label>
              输出变量（逗号分隔）
              <input v-model="exportText" placeholder="user_id_list, ..." />
            </label>
            <label>
              上游变量映射（JSON）
              <textarea v-model="mappingText" rows="5" spellcheck="false" />
              <span v-if="mappingError" class="field-error">{{ mappingError }}</span>
            </label>
            <label>
              失败策略
              <select v-model="selectedNode.config.fail_strategy">
                <option value="abort_dag">失败时终止 DAG</option>
                <option value="continue">失败时继续</option>
              </select>
            </label>
          </form>
        </section>

        <section class="panel">
          <h2>添加依赖连线</h2>
          <div class="connect-form">
            <label>
              上游节点
              <select v-model="sourceNodeID">
                <option value="" disabled>选择来源</option>
                <option v-for="node in dag.nodes" :key="node.node_id" :value="node.node_id">{{ node.node_name }}</option>
              </select>
            </label>
            <label>
              下游节点
              <select v-model="targetNodeID">
                <option value="" disabled>选择目标</option>
                <option v-for="node in dag.nodes" :key="node.node_id" :value="node.node_id">{{ node.node_name }}</option>
              </select>
            </label>
            <button class="secondary-button" type="button" @click="connectNodes">添加连线</button>
          </div>
          <ul v-if="dag.edges.length" class="edge-list">
            <li v-for="edge in dag.edges" :key="edge.edge_id">
              <span>{{ edge.source_node_id }} → {{ edge.target_node_id }}</span>
              <button class="text-button" type="button" @click="dag.edges = dag.edges.filter((item) => item.edge_id !== edge.edge_id)">移除</button>
            </li>
          </ul>
        </section>
      </aside>
    </section>

    <section class="panel json-panel">
      <div class="json-heading">
        <div>
          <h2>dag_content</h2>
          <p class="muted">以下 JSON 即画布序列化内容，可作为 data_dag.dag_content 保存。</p>
        </div>
      </div>
      <pre><code>{{ dagContent }}</code></pre>
    </section>
  </main>
</template>
