<template>
  <section class="recycle-panel" data-testid="recycle-panel">
    <header class="recycle-head">
      <h3>{{ isTimeTravel ? '时间旅行 · 重试读取与出错快照' : '保留策略详情 · 回收候选与回收单对账' }}</h3>
      <p class="recycle-tip">
        {{ isTimeTravel
          ? '按快照编号重试历史读取；历史文件异常丢失时会同步列出出错快照。'
          : '先核对回收候选（仍被新快照引用的文件不会回收），提交后留存带具体路径的删除回执，可用原回收编号对账。' }}
      </p>
    </header>

    <div class="recycle-form">
      <label class="rc-field">
        <span>所属湖表</span>
        <input v-model="table" data-testid="rc-table" placeholder="例如 lake_demo" />
      </label>

      <label v-if="!isTimeTravel" class="rc-field rc-grow">
        <span>快照编号（多个用逗号分隔，可含不存在编号）</span>
        <input v-model="snapshots" data-testid="rc-snapshots" placeholder="snap-old, snap-missing" />
      </label>
      <label v-else class="rc-field rc-grow">
        <span>快照编号</span>
        <input v-model="snapshotId" data-testid="tt-snapshot" placeholder="snap-xxxx" />
      </label>

      <label v-if="!isTimeTravel" class="rc-field">
        <span>回收编号（幂等键）</span>
        <input v-model="recycleId" data-testid="rc-recycle-id" placeholder="RC-0001" />
      </label>
    </div>

    <div class="recycle-actions">
      <template v-if="!isTimeTravel">
        <button class="btn" type="button" data-testid="rc-candidates" @click="loadCandidates">核对回收候选</button>
        <button class="btn primary" type="button" data-testid="rc-submit" @click="submitCleanup">提交清理</button>
        <button class="btn" type="button" data-testid="rc-reconcile" @click="reconcile">用原回收编号对账</button>
      </template>
      <template v-else>
        <button class="btn primary" type="button" data-testid="tt-read" @click="timeRead">从时间旅行入口重试读取</button>
      </template>
      <button class="btn ghost" type="button" data-testid="rc-errors" @click="loadErrors">核对出错快照</button>
    </div>

    <p v-if="message" class="rc-message" :class="{ 'rc-error': hasError }" data-testid="rc-message">{{ message }}</p>

    <div v-if="candidates.length" class="rc-block" data-testid="rc-candidate-list">
      <h4>回收候选（{{ candidates.length }}）· 预计可回收 {{ candidateBytes }} 字节</h4>
      <ul>
        <li v-for="(item, idx) in candidates" :key="`${item.file_id}-${idx}`" data-testid="rc-candidate-item">
          <code>{{ item.file_id }}</code>
          <span class="rc-badge" :data-disposition="item.disposition">{{ dispositionText(item.disposition) }}</span>
          <span v-if="item.file_bytes != null">{{ item.file_bytes }} 字节</span>
          <code class="rc-path">{{ item.path }}</code>
        </li>
      </ul>
    </div>

    <div v-if="receipts.length" class="rc-block" data-testid="rc-receipts">
      <h4>删除回执（{{ receipts.length }}）· 实际释放 {{ releasedBytes }} 字节</h4>
      <ul>
        <li v-for="rc in receipts" :key="rc.file_id" data-testid="rc-receipt-item">
          <code>{{ rc.file_id }}</code>
          <span>{{ rc.file_bytes }} 字节</span>
          <span v-if="rc.recycle_id" class="rc-rid">{{ rc.recycle_id }}</span>
          <code class="rc-path" data-testid="rc-receipt-path">{{ rc.path || rc.file_path }}</code>
        </li>
      </ul>
    </div>

    <div v-if="errorSnapshots.length" class="rc-block rc-errors" data-testid="rc-error-snapshots">
      <h4>出错快照（{{ errorSnapshots.length }}）</h4>
      <ul>
        <li v-for="es in errorSnapshots" :key="es.snapshot_id" data-testid="rc-error-snapshot-item">
          <code>{{ es.snapshot_id }}</code>
          <span>丢失文件：{{ (es.missing_file_ids || []).join(', ') }}</span>
        </li>
      </ul>
    </div>

    <pre v-if="output" class="rc-output" data-testid="rc-output">{{ output }}</pre>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

import { request } from '@/api/client'

const props = defineProps<{ moduleKey: string }>()
const isTimeTravel = computed(() => props.moduleKey === 'time_travel')

const table = ref('')
const snapshots = ref('')
const snapshotId = ref('')
const recycleId = ref('')

const message = ref('')
const hasError = ref(false)
const output = ref('')
const candidates = ref<any[]>([])
const receipts = ref<any[]>([])
const errorSnapshots = ref<any[]>([])
const releasedBytes = ref(0)
const candidateBytes = ref(0)

function snapshotIdList(): string[] {
  return snapshots.value
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
}

function show(data: unknown, note = '') {
  output.value = JSON.stringify(data, null, 2)
  if (note) message.value = note
}

function fail(error: unknown, fallback: string) {
  hasError.value = true
  message.value = error instanceof Error ? error.message : fallback
}

async function readJson(response: Response): Promise<any> {
  const data = await response.json().catch(() => null)
  if (!response.ok) {
    const detail = data?.detail || data?.message || `接口返回 ${response.status}`
    throw new Error(typeof detail === 'string' ? detail : JSON.stringify(detail))
  }
  return data
}

function dispositionText(d: string): string {
  if (d === 'reclaimable') return '可回收'
  if (d === 'referenced') return '仍被引用·保留'
  if (d === 'already_recycled') return '已回收'
  if (d === 'file_missing') return '文件缺失'
  return d
}

async function loadCandidates() {
  hasError.value = false
  message.value = ''
  try {
    const params = new URLSearchParams({ lake_table: table.value })
    const ids = snapshotIdList()
    if (ids.length) params.set('snapshot_ids', ids.join(','))
    const data = await readJson(await request(`/api/expire_policy/candidates?${params.toString()}`))
    candidates.value = data.candidates || data.items || []
    candidateBytes.value = Number(data.reclaimable_bytes || 0)
    show(data, `候选核对完成：可回收 ${candidateBytes.value} 字节`)
  } catch (error) {
    fail(error, '回收候选查询失败')
  }
}

async function submitCleanup() {
  hasError.value = false
  message.value = ''
  try {
    const payload: Record<string, unknown> = {
      lake_table: table.value,
      snapshot_ids: snapshotIdList(),
    }
    if (recycleId.value.trim()) payload.recycle_id = recycleId.value.trim()
    const data = await readJson(await request('/api/recycle/orders', {
      method: 'POST',
      body: JSON.stringify({ values: payload }),
    }))
    receipts.value = data.receipts || []
    releasedBytes.value = Number(data.released_bytes || 0)
    if (data.recycle_id && !recycleId.value) recycleId.value = data.recycle_id
    show(data, data.idempotent_replay
      ? `同一回收编号重复提交，返回首次回收单（${releasedBytes.value} 字节）`
      : `清理完成：实际释放 ${releasedBytes.value} 字节`)
    await loadErrorsSilent()
  } catch (error) {
    fail(error, '清理提交失败')
  }
}

async function reconcile() {
  hasError.value = false
  message.value = ''
  try {
    const rid = recycleId.value.trim()
    if (!rid) throw new Error('请填写原回收编号')
    const params = new URLSearchParams()
    if (table.value) params.set('lake_table', table.value)
    const qs = params.toString()
    const data = await readJson(
      await request(`/api/recycle/orders/${encodeURIComponent(rid)}${qs ? `?${qs}` : ''}`),
    )
    receipts.value = data.receipts || []
    releasedBytes.value = Number(data.released_bytes || 0)
    show(data, data.aligned ? '对账一致：删除回执均可核回' : '对账发现差异，请查看明细')
  } catch (error) {
    fail(error, '对账失败')
  }
}

async function timeRead() {
  hasError.value = false
  message.value = ''
  try {
    if (!table.value || !snapshotId.value.trim()) throw new Error('请填写湖表与快照编号')
    const params = new URLSearchParams({ lake_table: table.value })
    const data = await readJson(await request(
      `/api/time_travel/read/${encodeURIComponent(snapshotId.value.trim())}?${params.toString()}`,
    ))
    if (data.success) {
      message.value = `读取成功：${data.rows} 行，命中 ${data.partition_count} 个分区`
    } else {
      hasError.value = true
      message.value = data.error_message || data.error || '读取失败：历史文件缺失'
      errorSnapshots.value = data.errored_snapshots
        || (data.error_snapshot_ids || []).map((sid: string) => ({ snapshot_id: sid }))
    }
    show(data)
    await loadErrorsSilent()
  } catch (error) {
    fail(error, '时间旅行读取失败')
  }
}

async function loadErrors() {
  await loadErrorsSilent(true)
}

async function loadErrorsSilent(announce = false) {
  try {
    const params = new URLSearchParams()
    if (table.value) params.set('lake_table', table.value)
    const qs = params.toString()
    const data = await readJson(await request(`/api/time_travel/errors${qs ? `?${qs}` : ''}`))
    errorSnapshots.value = data.error_snapshots || data.items || []
    if (announce) {
      hasError.value = errorSnapshots.value.length > 0
      message.value = errorSnapshots.value.length
        ? `发现 ${errorSnapshots.value.length} 个出错快照`
        : '没有因历史文件丢失而出错的快照'
      show(data)
    }
  } catch (error) {
    if (announce) fail(error, '出错快照查询失败')
  }
}
</script>

<style scoped>
.recycle-panel {
  margin: 16px 0;
  padding: 16px;
  border: 1px solid var(--border-color, #e3e8ef);
  border-radius: 12px;
  background: var(--panel-bg, #fbfcfe);
}
.recycle-head h3 { margin: 0 0 4px; font-size: 16px; }
.recycle-tip { margin: 0 0 12px; color: #667085; font-size: 13px; }
.recycle-form { display: flex; flex-wrap: wrap; gap: 12px; }
.rc-field { display: flex; flex-direction: column; gap: 4px; font-size: 13px; }
.rc-grow { flex: 1 1 260px; }
.rc-field input { padding: 6px 10px; border: 1px solid #d0d5dd; border-radius: 8px; min-width: 180px; }
.recycle-actions { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }
.rc-message { font-size: 13px; color: #175cd3; }
.rc-message.rc-error { color: #b42318; }
.rc-block { margin-top: 10px; font-size: 13px; }
.rc-block ul { margin: 6px 0 0; padding-left: 18px; display: flex; flex-direction: column; gap: 4px; }
.rc-path { color: #475467; margin-left: 8px; word-break: break-all; }
.rc-badge { padding: 1px 8px; border-radius: 999px; background: #eef2f6; font-size: 12px; }
.rc-badge[data-disposition='reclaimable'] { background: #fef0c7; color: #93370d; }
.rc-badge[data-disposition='referenced'] { background: #d1fadf; color: #05603a; }
.rc-rid { color: #175cd3; margin-left: 8px; }
.rc-errors h4 { color: #b42318; }
.rc-output {
  margin-top: 10px; padding: 10px; max-height: 280px; overflow: auto;
  background: #101828; color: #d0d5dd; border-radius: 8px; font-size: 12px;
}
</style>
