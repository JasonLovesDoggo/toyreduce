<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import WorkerStatus from '$lib/components/WorkerStatus.svelte';
	import JobList from '$lib/components/JobList.svelte';
	import JobSubmitForm from '$lib/components/JobSubmitForm.svelte';
	import StoreVisualization from '$lib/components/StoreVisualization.svelte';
	import ResultsViewer from '$lib/components/ResultsViewer.svelte';
	import { formatRelativeTime } from '$lib/utils/date';

	const API_BASE = 'http://localhost:8080';

	let jobs = $state<any[]>([]);
	let workers = $state<any[]>([]);
	let config = $state<any>({});
	let loading = $state(true);
	let error = $state('');

	// Get active tab and job from URL query parameters
	let activeTab = $derived<'jobs' | 'results' | 'store' | 'workers'>(
		(page.url.searchParams.get('tab') as 'jobs' | 'results' | 'store' | 'workers') || 'jobs'
	);
	let selectedJobID = $derived(page.url.searchParams.get('job') || '');

	function setTab(tab: 'jobs' | 'results' | 'store' | 'workers') {
		goto(`?tab=${tab}`, { replaceState: false, keepFocus: true });
	}

	function viewJobResults(jobId: string) {
		goto(`?tab=results&job=${jobId}`, { replaceState: false, keepFocus: true });
	}

	async function fetchData() {
		try {
			// Fetch jobs
			const jobsRes = await fetch(`${API_BASE}/api/jobs`);
			if (jobsRes.ok) {
				const data = await jobsRes.json();
				jobs = data.jobs || [];
			}

			// Fetch workers
			const workersRes = await fetch(`${API_BASE}/api/workers`);
			if (workersRes.ok) {
				const data = await workersRes.json();
				workers = data.workers || [];
			}

			// Fetch config
			const configRes = await fetch(`${API_BASE}/api/config`);
			if (configRes.ok) {
				config = await configRes.json();
			}

			error = '';
		} catch (err) {
			error = `Failed to connect to master at ${API_BASE}. Make sure the master is running.`;
			console.error(err);
		} finally {
			loading = false;
		}
	}

	async function handleJobSubmit(jobData: any) {
		try {
			const res = await fetch(`${API_BASE}/api/jobs`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(jobData)
			});

			if (res.ok) {
				fetchData(); // Refresh job list
			} else {
				const data = await res.json();
				alert(`Failed to submit job: ${data.message || 'Unknown error'}`);
			}
		} catch (err) {
			alert(`Failed to submit job: ${err}`);
		}
	}

	async function handleJobCancel(jobId: string) {
		if (!confirm('Are you sure you want to cancel this job?')) return;

		try {
			const res = await fetch(`${API_BASE}/api/jobs/${jobId}/cancel`, {
				method: 'POST'
			});

			if (res.ok) {
				fetchData(); // Refresh job list
			} else {
				const data = await res.json();
				alert(`Failed to cancel job: ${data.message || 'Unknown error'}`);
			}
		} catch (err) {
			alert(`Failed to cancel job: ${err}`);
		}
	}

	onMount(() => {
		fetchData();
		// Refresh every 2 seconds
		const interval = setInterval(fetchData, 2000);
		return () => clearInterval(interval);
	});
</script>

<div>
	<!-- Tabs -->
	<div class="mb-8 border-b border-[var(--border)]">
		<div class="flex gap-8">
			<button
				onclick={() => setTab('jobs')}
				class="tab pb-3 text-sm tracking-wider uppercase {activeTab === 'jobs'
					? 'active text-[var(--fg)]'
					: 'text-[var(--text-muted)]'}"
			>
				Jobs
			</button>
			<button
				onclick={() => setTab('results')}
				class="tab pb-3 text-sm tracking-wider uppercase {activeTab === 'results'
					? 'active text-[var(--fg)]'
					: 'text-[var(--text-muted)]'}"
			>
				Results
			</button>
			<button
				onclick={() => setTab('store')}
				class="tab pb-3 text-sm tracking-wider uppercase {activeTab === 'store'
					? 'active text-[var(--fg)]'
					: 'text-[var(--text-muted)]'}"
			>
				Store
			</button>
			<button
				onclick={() => setTab('workers')}
				class="tab pb-3 text-sm tracking-wider uppercase {activeTab === 'workers'
					? 'active text-[var(--fg)]'
					: 'text-[var(--text-muted)]'}"
			>
				Workers
			</button>
		</div>
	</div>

	<!-- Error Message -->
	{#if error}
		<div class="mb-8 border border-red-500 bg-red-50 p-4">
			<p class="text-sm text-red-800">{error}</p>
		</div>
	{/if}

	<!-- Loading State -->
	{#if loading}
		<div class="py-16 text-center">
			<div
				class="inline-block h-6 w-6 animate-spin border-2 border-[var(--border)] border-t-[var(--accent)]"
			></div>
			<p class="mt-4 text-sm text-[var(--text-muted)]">Loading...</p>
		</div>
	{:else}
		<!-- Tab Content -->
		{#if activeTab === 'jobs'}
			<div class="space-y-8">
				<WorkerStatus {workers} />
				<JobSubmitForm onSubmit={handleJobSubmit} executors={config.executors} />
				<JobList
					{jobs}
					onCancel={handleJobCancel}
					onRefresh={fetchData}
					{API_BASE}
					onViewResults={viewJobResults}
				/>
			</div>
		{:else if activeTab === 'results'}
			<ResultsViewer {API_BASE} />
		{:else if activeTab === 'store'}
			<StoreVisualization {API_BASE} />
		{:else if activeTab === 'workers'}
			<div class="space-y-8">
				<!-- Config Info -->
				<div class="border border-[var(--border)] bg-[var(--surface)] p-8">
					<h2 class="mb-6 text-lg font-semibold text-[var(--fg)]">Configuration</h2>
					<div class="grid grid-cols-2 gap-6">
						<div>
							<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
								Store URL
							</div>
							<div class="font-mono text-sm text-[var(--fg)]">{config.store_url || '—'}</div>
						</div>
						<div>
							<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
								Heartbeat Timeout
							</div>
							<div class="font-mono text-sm text-[var(--fg)]">
								{config.heartbeat_timeout || '—'}
							</div>
						</div>
					</div>
				</div>

				<!-- Worker List -->
				<div class="border border-[var(--border)] bg-[var(--surface)] p-8">
					<h2 class="mb-6 text-lg font-semibold text-[var(--fg)]">Worker Nodes</h2>
					{#if workers.length === 0}
						<p class="text-sm text-[var(--text-muted)]">No workers connected.</p>
					{:else}
						<div class="space-y-4">
							{#each workers as worker}
								<div class="border border-[var(--border)] p-4">
									<div class="flex items-center justify-between">
										<div class="flex items-center gap-3">
											<!-- Health Indicator -->
											<div class="relative">
												{#if worker.online}
													<div class="h-3 w-3 rounded-full bg-green-500"></div>
													<div
														class="absolute inset-0 h-3 w-3 animate-ping rounded-full bg-green-500 opacity-75"
													></div>
												{:else}
													<div class="h-3 w-3 rounded-full bg-red-500"></div>
												{/if}
											</div>
											<div>
												<div class="font-mono text-sm text-[var(--fg)]">{worker.id}</div>
												<div class="mt-0.5 text-xs text-[var(--text-muted)]">
													{worker.online
														? `Last seen ${formatRelativeTime(worker.last_heartbeat)}`
														: 'Offline'}
												</div>
											</div>
										</div>
										<div class="text-right">
											<div class="mb-1 text-xs tracking-wider text-[var(--text-muted)] uppercase">
												Executors
											</div>
											<div class="text-xs text-[var(--fg)]">
												{worker.executors?.join(', ') || 'None'}
											</div>
										</div>
									</div>
									{#if worker.current_task}
										<div class="mt-3 border-t border-[var(--border)] pt-3">
											<div class="mb-1 text-xs tracking-wider text-[var(--text-muted)] uppercase">
												Current Task
											</div>
											<div class="font-mono text-xs text-[var(--fg)]">{worker.current_task}</div>
										</div>
									{/if}
								</div>
							{/each}
						</div>
					{/if}
				</div>
			</div>
		{/if}
	{/if}
</div>
