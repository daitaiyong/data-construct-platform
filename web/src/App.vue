<script setup>
import { ref } from 'vue'

const apiBase = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8080'
const apiStatus = ref('未检查')

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
</script>

<template>
  <main class="page">
    <header>
      <p class="eyebrow">DATA CONSTRUCT PLATFORM</p>
      <h1>数据构造平台</h1>
      <p class="intro">模板管理、DAG 编排与任务运行状态的工作台骨架。</p>
    </header>

    <section class="card">
      <div>
        <h2>服务连接</h2>
        <p>API：<code>{{ apiBase }}</code></p>
        <p>状态：<strong>{{ apiStatus }}</strong></p>
      </div>
      <button type="button" @click="checkAPI">检查 API</button>
    </section>

    <section class="grid">
      <article class="card">
        <h2>模板管理</h2>
        <p>模板列表与编辑流程待接入 API。</p>
      </article>
      <article class="card">
        <h2>DAG 画布</h2>
        <p>节点依赖校验 API 已提供；可视化编排待实现。</p>
      </article>
      <article class="card">
        <h2>任务监控</h2>
        <p>任务创建与状态查询 API 已提供。</p>
      </article>
    </section>
  </main>
</template>
