<script lang="ts">
	let { workers = [] } = $props<{ workers: any[] }>();
</script>

<div class="mb-8 border border-[var(--border)] bg-[var(--surface)] p-8">
	<h2 class="mb-6 text-lg font-semibold text-[var(--fg)]">Cluster</h2>

	<div class="grid grid-cols-3 gap-8">
		<!-- Workers -->
		<div>
			<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">Workers</div>
			<div class="text-4xl font-semibold text-[var(--fg)] tabular-nums">{workers.length}</div>
			<div class="mt-1 text-xs text-[var(--text-muted)]">
				{workers.filter((w: any) => w.online).length} online
			</div>
		</div>

		<!-- Status -->
		<div>
			<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">Status</div>
			<div class="text-2xl font-semibold text-[var(--fg)]">
				{workers.length > 0 ? 'Ready' : 'Idle'}
			</div>
		</div>

		<!-- Executors -->
		<div>
			<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">Executors</div>
			<div class="text-sm text-[var(--fg)]">
				{#if workers.length > 0}
					{[...new Set(workers.flatMap((w: any) => w.executors || []))].join(', ') || '—'}
				{:else}
					<span class="text-[var(--text-muted)]">—</span>
				{/if}
			</div>
		</div>
	</div>
</div>
