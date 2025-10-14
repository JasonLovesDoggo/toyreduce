<script lang="ts">
	import { getContext } from 'svelte';
	import JobDetails from './JobDetails.svelte';
	import type { Context } from 'svelte-simple-modal';

	const { open } = getContext<Context>('simple-modal');

	interface Props {
		jobs?: any[];
		API_BASE?: string;
		onCancel: (jobId: string) => void;
		onRefresh?: () => void;
	}

	let { jobs = [], API_BASE = '', onCancel }: Props = $props();

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

	function showDetails(job: any) {
		// @ts-expect-error - svelte-simple-modal not yet compatible with Svelte 5 component types
		open(JobDetails, {
			job,
			API_BASE,
			onCancel: () => {
				onCancel(job.id);
			}
		});
	}

	function getProgressPercent(job: any) {
		if (!job.total_tasks || job.total_tasks === 0) return 0;
		return Math.round((job.completed_tasks / job.total_tasks) * 100);
	}
</script>

<div class="border border-[var(--border)] bg-[var(--surface)] p-8">
	<h2 class="mb-6 text-lg font-semibold text-[var(--fg)]">Jobs</h2>

	{#if jobs.length === 0}
		<p class="py-8 text-center text-sm text-[var(--text-muted)]">
			No jobs yet. Submit your first job above!
		</p>
	{:else}
		<div class="space-y-3">
			{#each jobs as job (job.id)}
				<div
					role="button"
					tabindex="0"
					class="cursor-pointer border border-[var(--border)] bg-[var(--surface)] p-4 transition-colors hover:border-[var(--accent)]"
					onclick={() => showDetails(job)}
					onkeydown={(e) => e.key === 'Enter' && showDetails(job)}
				>
					<div class="flex items-start justify-between">
						<div class="flex-1">
							<div class="mb-2 flex items-center gap-3">
								<span class="font-mono text-xs text-[var(--text-muted)]">
									{job.id.slice(0, 8)}
								</span>
								<span
									class={`border px-2 py-0.5 text-xs tracking-wider uppercase ${getStatusColor(job.status)}`}
								>
									{job.status}
								</span>
								<span class="text-xs text-[var(--text-muted)]">{job.executor}</span>
							</div>

							<div class="font-mono text-sm text-[var(--fg)]">
								{job.input_path}
							</div>

							{#if job.status === 'running'}
								<div class="mt-3">
									<div
										class="mb-2 flex items-center gap-2 text-xs text-[var(--text-muted)] tabular-nums"
									>
										<span>Progress: {getProgressPercent(job)}%</span>
										<span>•</span>
										<span>Map: {job.map_tasks_done}/{job.map_tasks_total}</span>
										<span>•</span>
										<span>Reduce: {job.reduce_tasks_done}/{job.reduce_tasks_total}</span>
									</div>
									<div class="h-1 w-full bg-[var(--border)]">
										<div
											class="h-1 bg-[var(--accent)] transition-all"
											style="width: {getProgressPercent(job)}%"
										></div>
									</div>
								</div>
							{/if}
						</div>

						<div class="text-right text-xs text-[var(--text-muted)]">
							{formatDate(job.submitted_at)}
						</div>
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
