<script lang="ts">
	let { API_BASE = '' } = $props<{ API_BASE: string }>();

	let stats = $state<any>(null);
	let health = $state<any>(null);
	let loading = $state(true);
	let actionLoading = $state('');

	async function fetchCacheData() {
		try {
			// Fetch stats
			const statsRes = await fetch(`${API_BASE}/api/cache/stats`);
			if (statsRes.ok) {
				stats = await statsRes.json();
			}

			// Fetch health
			const healthRes = await fetch(`${API_BASE}/api/cache/health`);
			if (healthRes.ok) {
				health = await healthRes.json();
			}
		} catch (err) {
			console.error('Failed to fetch cache data:', err);
		} finally {
			loading = false;
		}
	}

	async function handleReset() {
		if (
			!confirm(
				'Are you sure you want to reset the cache? This will delete all intermediate and final data.'
			)
		) {
			return;
		}

		actionLoading = 'reset';
		try {
			const res = await fetch(`${API_BASE}/api/cache/reset`, { method: 'POST' });
			if (res.ok) {
				alert('Cache reset successfully');
				fetchCacheData();
			} else {
				const data = await res.json();
				alert(`Failed to reset cache: ${data.error || 'Unknown error'}`);
			}
		} catch (err) {
			alert(`Failed to reset cache: ${err}`);
		} finally {
			actionLoading = '';
		}
	}

	async function handleCompact() {
		actionLoading = 'compact';
		try {
			const res = await fetch(`${API_BASE}/api/cache/compact`, { method: 'POST' });
			if (res.ok) {
				alert('Cache compacted successfully');
				fetchCacheData();
			} else {
				const data = await res.json();
				alert(`Failed to compact cache: ${data.error || 'Unknown error'}`);
			}
		} catch (err) {
			alert(`Failed to compact cache: ${err}`);
		} finally {
			actionLoading = '';
		}
	}

	// Fetch on mount
	$effect(() => {
		fetchCacheData();
	});
</script>

<div class="space-y-8">
	<!-- Health Status -->
	<div class="border border-[var(--border)] bg-[var(--surface)] p-8">
		<h2 class="mb-6 text-lg font-semibold text-[var(--fg)]">Cache Health</h2>
		{#if loading}
			<p class="text-sm text-[var(--text-muted)]">Loading...</p>
		{:else if health}
			<div class="flex items-center gap-3">
				<div class="relative">
					{#if health.healthy}
						<div class="h-4 w-4 rounded-full bg-green-500"></div>
						<div
							class="absolute inset-0 h-4 w-4 animate-ping rounded-full bg-green-500 opacity-75"
						></div>
					{:else}
						<div class="h-4 w-4 rounded-full bg-red-500"></div>
					{/if}
				</div>
				<div>
					<div class="text-sm font-semibold text-[var(--fg)]">
						{health.healthy ? 'Healthy' : 'Unhealthy'}
					</div>
					{#if health.error}
						<div class="mt-1 text-xs text-red-600">{health.error}</div>
					{/if}
				</div>
			</div>
		{:else}
			<p class="text-sm text-[var(--text-muted)]">Unable to check cache health</p>
		{/if}
	</div>

	<!-- Statistics -->
	{#if stats}
		<div class="border border-[var(--border)] bg-[var(--surface)] p-8">
			<h2 class="mb-6 text-lg font-semibold text-[var(--fg)]">Statistics</h2>
			<div class="grid grid-cols-3 gap-8">
				<div>
					<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
						Intermediate Keys
					</div>
					<div class="text-3xl font-semibold text-[var(--fg)] tabular-nums">
						{stats.intermediate_kvs || 0}
					</div>
				</div>
				<div>
					<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
						Reduce Tasks
					</div>
					<div class="text-3xl font-semibold text-[var(--fg)] tabular-nums">
						{stats.reduce_tasks || 0}
					</div>
				</div>
				<div>
					<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
						Total Size
					</div>
					<div class="text-3xl font-semibold text-[var(--fg)] tabular-nums">
						{stats.db_size_bytes ? (stats.db_size_bytes / 1024 / 1024).toFixed(2) : 0} MB
					</div>
				</div>
			</div>
		</div>
	{/if}

	<!-- Management Actions -->
	<div class="border border-[var(--border)] bg-[var(--surface)] p-8">
		<h2 class="mb-6 text-lg font-semibold text-[var(--fg)]">Management</h2>
		<div class="flex gap-4">
			<button
				onclick={handleCompact}
				disabled={actionLoading !== ''}
				class="border border-[var(--border)] bg-[var(--surface)] px-6 py-2 text-xs tracking-wider text-[var(--fg)] uppercase transition-colors hover:border-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-30"
			>
				{actionLoading === 'compact' ? 'Compacting...' : 'Compact'}
			</button>
			<button
				onclick={handleReset}
				disabled={actionLoading !== ''}
				class="bg-red-600 px-6 py-2 text-xs tracking-wider text-white uppercase transition-opacity hover:opacity-80 disabled:cursor-not-allowed disabled:opacity-30"
			>
				{actionLoading === 'reset' ? 'Resetting...' : 'Reset Cache'}
			</button>
		</div>
		<p class="mt-4 text-xs text-[var(--text-muted)]">
			<strong>Compact:</strong> Optimize database storage without losing data.<br />
			<strong>Reset:</strong> Delete all intermediate and final cached data. Use with caution.
		</p>
	</div>
</div>
