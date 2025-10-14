<script lang="ts">
	import { getContext } from 'svelte';

	let { job, API_BASE, onCancel } = $props<{ job: any; API_BASE: string; onCancel: () => void }>();

	const { close } = getContext('simple-modal');

	let jobResults = $state<any[]>([]);
	let loadingResults = $state(false);

	// Load results when component mounts if job is completed
	$effect(() => {
		if (job.status === 'completed') {
			loadResults();
		}
	});

	async function loadResults() {
		loadingResults = true;
		try {
			const res = await fetch(`${API_BASE}/api/jobs/${job.id}/results`);
			if (res.ok) {
				jobResults = await res.json();
			}
		} catch (err) {
			console.error('Failed to load results:', err);
		} finally {
			loadingResults = false;
		}
	}

	function getStatusColor(status: string) {
		switch (status) {
			case 'queued':
				return 'border-[var(--border)] text-[var(--text-muted)]';
			case 'running':
				return 'border-[var(--accent)] text-[var(--accent)]';
			case 'completed':
				return 'border-[var(--fg)] text-[var(--fg)]';
			case 'failed':
				return 'border-red-500 text-red-600';
			case 'cancelled':
				return 'border-[var(--text-muted)] text-[var(--text-muted)]';
			default:
				return 'border-[var(--border)] text-[var(--text-muted)]';
		}
	}

	function formatDate(date: string) {
		return new Date(date).toLocaleString();
	}

	function formatDuration(seconds: number): string {
		if (!seconds || seconds === 0) return '—';

		const hours = Math.floor(seconds / 3600);
		const minutes = Math.floor((seconds % 3600) / 60);
		const secs = Math.floor(seconds % 60);

		if (hours > 0) {
			return `${hours}h ${minutes}m ${secs}s`;
		} else if (minutes > 0) {
			return `${minutes}m ${secs}s`;
		} else {
			return `${secs}s`;
		}
	}

	function formatTaskThroughput(tasks: number, seconds: number): string {
		if (!seconds || seconds === 0 || !tasks) return '—';
		const throughput = tasks / seconds;
		return throughput < 1 ? throughput.toFixed(2) : throughput.toFixed(1);
	}

	function getProgressPercent(job: any) {
		if (!job.total_tasks || job.total_tasks === 0) return 0;
		return Math.round((job.completed_tasks / job.total_tasks) * 100);
	}

	function handleCancel() {
		onCancel();
		close();
	}
</script>

<!-- Header -->
<div class="sticky top-0 z-10 border-b border-[var(--border)] bg-[var(--bg)] px-8 py-6">
	<div class="flex items-start justify-between">
		<div class="flex-1">
			<div class="mb-3 flex items-center gap-3">
				<h3 class="text-xl font-bold text-[var(--fg)]">Job Details</h3>
				<span
					class={`border px-3 py-1 text-xs font-semibold tracking-wider uppercase ${getStatusColor(job.status)}`}
				>
					{job.status}
				</span>
			</div>
			<p class="font-mono text-sm text-[var(--text-muted)]">{job.id}</p>
		</div>
		<button
			onclick={close}
			class="rounded p-2 text-[var(--text-muted)] transition-colors hover:bg-[var(--surface)] hover:text-[var(--fg)]"
		>
			<svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path
					stroke-linecap="round"
					stroke-linejoin="round"
					stroke-width="2"
					d="M6 18L18 6M6 6l12 12"
				/>
			</svg>
		</button>
	</div>
</div>

<div class="space-y-6 p-8">
	<!-- Hero Section - Duration -->
	{#if job.duration}
		<div class="border border-[var(--accent)] bg-[var(--surface)] p-8 text-center">
			<div class="mb-2 text-xs font-semibold tracking-wider text-[var(--text-muted)] uppercase">
				Total Duration
			</div>
			<div class="text-5xl font-bold tracking-tight text-[var(--fg)] tabular-nums">
				{formatDuration(job.duration)}
			</div>
			{#if job.status === 'running'}
				<div class="mt-3 text-sm text-[var(--text-muted)]">Job in progress...</div>
			{/if}
		</div>
	{/if}

	<!-- Metrics Cards Grid -->
	{#if job.status === 'running' || job.status === 'completed'}
		<div class="grid grid-cols-2 gap-4">
			<!-- Map Phase Card -->
			<div class="border border-[var(--border)] bg-[var(--surface)] p-6">
				<div class="mb-4 flex items-center justify-between">
					<h4 class="text-sm font-semibold tracking-wider text-[var(--fg)] uppercase">
						Map Phase
					</h4>
					<svg
						class="h-5 w-5 text-[var(--text-muted)]"
						fill="none"
						stroke="currentColor"
						viewBox="0 0 24 24"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							stroke-width="2"
							d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
						/>
					</svg>
				</div>
				<div class="space-y-4">
					<div>
						<div class="text-3xl font-bold text-[var(--fg)] tabular-nums">
							{job.map_tasks_done}/{job.map_tasks_total}
						</div>
						<div class="mt-1 text-xs text-[var(--text-muted)]">Tasks Completed</div>
					</div>
					{#if job.map_phase_duration}
						<div class="border-t border-[var(--border)] pt-4">
							<div class="flex justify-between">
								<span class="text-xs text-[var(--text-muted)]">Duration</span>
								<span class="font-mono text-sm font-semibold text-[var(--fg)]"
									>{formatDuration(job.map_phase_duration)}</span
								>
							</div>
							<div class="mt-2 flex justify-between">
								<span class="text-xs text-[var(--text-muted)]">Throughput</span>
								<span class="font-mono text-sm font-semibold text-[var(--fg)]"
									>{formatTaskThroughput(job.map_tasks_done, job.map_phase_duration)} tasks/s</span
								>
							</div>
						</div>
					{/if}
				</div>
			</div>

			<!-- Reduce Phase Card -->
			<div class="border border-[var(--border)] bg-[var(--surface)] p-6">
				<div class="mb-4 flex items-center justify-between">
					<h4 class="text-sm font-semibold tracking-wider text-[var(--fg)] uppercase">
						Reduce Phase
					</h4>
					<svg
						class="h-5 w-5 text-[var(--text-muted)]"
						fill="none"
						stroke="currentColor"
						viewBox="0 0 24 24"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							stroke-width="2"
							d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"
						/>
					</svg>
				</div>
				<div class="space-y-4">
					<div>
						<div class="text-3xl font-bold text-[var(--fg)] tabular-nums">
							{job.reduce_tasks_done}/{job.reduce_tasks_total}
						</div>
						<div class="mt-1 text-xs text-[var(--text-muted)]">Tasks Completed</div>
					</div>
					{#if job.reduce_phase_duration}
						<div class="border-t border-[var(--border)] pt-4">
							<div class="flex justify-between">
								<span class="text-xs text-[var(--text-muted)]">Duration</span>
								<span class="font-mono text-sm font-semibold text-[var(--fg)]"
									>{formatDuration(job.reduce_phase_duration)}</span
								>
							</div>
							<div class="mt-2 flex justify-between">
								<span class="text-xs text-[var(--text-muted)]">Throughput</span>
								<span class="font-mono text-sm font-semibold text-[var(--fg)]"
									>{formatTaskThroughput(job.reduce_tasks_done, job.reduce_phase_duration)} tasks/s</span
								>
							</div>
						</div>
					{/if}
				</div>
			</div>
		</div>

		<!-- Progress Bar -->
		<div class="border border-[var(--border)] bg-[var(--surface)] p-6">
			<div class="mb-3 flex items-center justify-between">
				<span class="text-xs font-semibold tracking-wider text-[var(--text-muted)] uppercase"
					>Overall Progress</span
				>
				<span class="text-2xl font-bold text-[var(--accent)] tabular-nums"
					>{getProgressPercent(job)}%</span
				>
			</div>
			<div class="h-3 w-full bg-[var(--border)]">
				<div
					class="h-3 bg-[var(--accent)] transition-all"
					style="width: {getProgressPercent(job)}%"
				></div>
			</div>
			<div class="mt-3 flex justify-between text-xs text-[var(--text-muted)]">
				<span>{job.completed_tasks} / {job.total_tasks} tasks</span>
				<span>{job.total_tasks - job.completed_tasks} remaining</span>
			</div>
		</div>
	{/if}

	<!-- Job Info -->
	<div class="border border-[var(--border)] bg-[var(--surface)] p-6">
		<h4 class="mb-4 text-sm font-semibold tracking-wider text-[var(--fg)] uppercase">
			Job Information
		</h4>
		<div class="grid grid-cols-2 gap-4">
			<div>
				<div class="mb-1 text-xs tracking-wider text-[var(--text-muted)] uppercase">Executor</div>
				<div class="font-mono text-sm text-[var(--fg)]">{job.executor}</div>
			</div>
			<div>
				<div class="mb-1 text-xs tracking-wider text-[var(--text-muted)] uppercase">
					Chunk Size
				</div>
				<div class="font-mono text-sm text-[var(--fg)] tabular-nums">{job.chunk_size}</div>
			</div>
			<div class="col-span-2">
				<div class="mb-1 text-xs tracking-wider text-[var(--text-muted)] uppercase">
					Input Path
				</div>
				<div class="font-mono text-xs text-[var(--fg)]">{job.input_path}</div>
			</div>
		</div>
	</div>

	<!-- Timeline -->
	<div class="border border-[var(--border)] bg-[var(--surface)] p-6">
		<h4 class="mb-4 text-sm font-semibold tracking-wider text-[var(--fg)] uppercase">Timeline</h4>
		<div class="space-y-3">
			<div class="flex items-center gap-4">
				<div class="flex-shrink-0">
					<div class="h-2 w-2 rounded-full bg-[var(--text-muted)]"></div>
				</div>
				<div class="flex-1">
					<div class="text-xs text-[var(--text-muted)]">Submitted</div>
					<div class="font-mono text-xs text-[var(--fg)]">{formatDate(job.submitted_at)}</div>
				</div>
			</div>
			{#if job.started_at}
				<div class="flex items-center gap-4">
					<div class="flex-shrink-0">
						<div class="h-2 w-2 rounded-full bg-[var(--accent)]"></div>
					</div>
					<div class="flex-1">
						<div class="text-xs text-[var(--text-muted)]">Started</div>
						<div class="font-mono text-xs text-[var(--fg)]">{formatDate(job.started_at)}</div>
					</div>
				</div>
			{/if}
			{#if job.map_phase_completed_at}
				<div class="flex items-center gap-4">
					<div class="flex-shrink-0">
						<div class="h-2 w-2 rounded-full bg-[var(--accent)]"></div>
					</div>
					<div class="flex-1">
						<div class="text-xs text-[var(--text-muted)]">Map Phase Complete</div>
						<div class="font-mono text-xs text-[var(--fg)]">
							{formatDate(job.map_phase_completed_at)}
						</div>
					</div>
				</div>
			{/if}
			{#if job.completed_at}
				<div class="flex items-center gap-4">
					<div class="flex-shrink-0">
						<div class="h-2 w-2 rounded-full bg-green-500"></div>
					</div>
					<div class="flex-1">
						<div class="text-xs text-[var(--text-muted)]">Completed</div>
						<div class="font-mono text-xs text-[var(--fg)]">{formatDate(job.completed_at)}</div>
					</div>
				</div>
			{/if}
		</div>
	</div>

	<!-- Results -->
	{#if job.status === 'completed'}
		<div class="border border-[var(--border)] bg-[var(--surface)] p-4">
			<h4 class="mb-3 text-sm font-semibold tracking-wider text-[var(--fg)] uppercase">Results</h4>
			{#if loadingResults}
				<p class="py-4 text-center text-xs text-[var(--text-muted)]">Loading results...</p>
			{:else if jobResults.length > 0}
				<div class="max-h-64 overflow-y-auto">
					<table class="w-full text-xs">
						<thead class="sticky top-0 bg-[var(--bg)]">
							<tr class="border-b border-[var(--border)]">
								<th
									class="px-2 py-2 text-left font-semibold tracking-wider text-[var(--text-muted)] uppercase"
									>Key</th
								>
								<th
									class="px-2 py-2 text-left font-semibold tracking-wider text-[var(--text-muted)] uppercase"
									>Value</th
								>
							</tr>
						</thead>
						<tbody>
							{#each jobResults as result}
								<tr class="border-b border-[var(--border)]">
									<td class="px-2 py-2 font-mono text-[var(--fg)]">{result.key}</td>
									<td class="px-2 py-2 text-[var(--fg)] tabular-nums">{result.value}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<p class="py-4 text-center text-xs text-[var(--text-muted)]">No results available</p>
			{/if}
		</div>
	{/if}

	<!-- Error -->
	{#if job.error}
		<div class="border border-red-500 bg-[var(--surface)] p-4">
			<h4 class="mb-2 text-sm font-semibold tracking-wider text-red-600 uppercase">Error</h4>
			<p class="font-mono text-xs text-red-600">{job.error}</p>
		</div>
	{/if}

	<!-- Actions -->
	<div class="flex gap-3">
		{#if job.status === 'queued' || job.status === 'running'}
			<button
				onclick={handleCancel}
				class="flex-1 bg-red-600 px-4 py-2 text-xs tracking-wider text-white uppercase transition-opacity hover:opacity-80"
			>
				Cancel Job
			</button>
		{/if}
		<button
			onclick={close}
			class="flex-1 border border-[var(--border)] bg-[var(--surface)] px-4 py-2 text-xs tracking-wider text-[var(--fg)] uppercase transition-colors hover:border-[var(--accent)]"
		>
			Close
		</button>
	</div>
</div>