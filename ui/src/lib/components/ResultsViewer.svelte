<script lang="ts">
	import {
		createSvelteTable,
		getCoreRowModel,
		getSortedRowModel,
		getFilteredRowModel,
		getPaginationRowModel,
		type ColumnDef,
		type SortingState,
		type ColumnFiltersState
	} from '@tanstack/svelte-table';
	import { writable } from 'svelte/store';
	import { formatRelativeTime } from '$lib/utils/date';

	interface ResultRow {
		key: string;
		value: string;
	}

	interface Job {
		id: string;
		executor: string;
		status: string;
		created_at: string;
		completed_at?: string;
		map_total?: number;
		map_completed?: number;
		reduce_total?: number;
		reduce_completed?: number;
	}

	let { API_BASE = '' } = $props<{ API_BASE: string }>();

	let jobs = $state<Job[]>([]);
	let selectedJobId = $state<string | null>(null);
	let results = $state<ResultRow[]>([]);
	let loading = $state(true);
	let loadingResults = $state(false);
	let pageSize = $state(50);
	let searchInput = $state('');
	let debounceTimer: any = null;

	// Debounced search function
	function debouncedSetGlobalFilter(value: string) {
		if (debounceTimer) {
			clearTimeout(debounceTimer);
		}
		debounceTimer = setTimeout(() => {
			console.log('Applying filter:', value);
			setGlobalFilter(value);
		}, 300);
	}

	// Cleanup timer on component destroy
	$effect(() => {
		return () => {
			if (debounceTimer) {
				clearTimeout(debounceTimer);
			}
		};
	});

	// Handle search input with debouncing
	function handleSearchInput(event: Event) {
		const target = event.target as HTMLInputElement;
		searchInput = target.value;
		debouncedSetGlobalFilter(target.value);
	}

	// Fetch jobs list
	async function fetchJobs() {
		loading = true;
		try {
			const res = await fetch(`${API_BASE}/api/jobs`);
			if (res.ok) {
				const data = await res.json();
				jobs = (data.jobs || []).filter((j: Job) => j.status === 'completed');
			} else {
				console.error('Failed to fetch jobs:', res.statusText);
				jobs = [];
			}
		} catch (err) {
			console.error('Failed to fetch jobs:', err);
			jobs = [];
		} finally {
			loading = false;
		}
	}

	// Fetch results for a specific job
	async function fetchJobResults(jobId: string) {
		loadingResults = true;
		results = [];
		try {
			const res = await fetch(`${API_BASE}/api/jobs/${jobId}/results`);
			if (res.ok) {
				results = await res.json();
			} else {
				console.error('Failed to fetch results:', res.statusText);
				results = [];
			}
		} catch (err) {
			console.error('Failed to fetch results:', err);
			results = [];
		} finally {
			loadingResults = false;
		}
	}

	// Select a job and fetch its results
	function selectJob(jobId: string) {
		selectedJobId = jobId;
		fetchJobResults(jobId);
	}

	// Define columns
	const columns: ColumnDef<ResultRow>[] = [
		{
			accessorKey: 'key',
			header: 'Key',
			cell: (info) => info.getValue()
		},
		{
			accessorKey: 'value',
			header: 'Value',
			cell: (info) => info.getValue()
		}
	];

	// State management following TanStack docs pattern
	let sorting: SortingState = [];
	const setSorting = (updater: SortingState | ((old: SortingState) => SortingState)) => {
		if (updater instanceof Function) {
			sorting = updater(sorting);
		} else {
			sorting = updater;
		}
		options.update((old) => ({
			...old,
			state: {
				...old.state,
				sorting
			}
		}));
	};

	let columnFilters: ColumnFiltersState = $state([]);
	const setColumnFilters = (
		updater: ColumnFiltersState | ((old: ColumnFiltersState) => ColumnFiltersState)
	) => {
		if (updater instanceof Function) {
			columnFilters = updater(columnFilters);
		} else {
			columnFilters = updater;
		}
		options.update((old) => ({
			...old,
			state: {
				...old.state,
				columnFilters
			}
		}));
	};

	let globalFilter = $state('');
	const setGlobalFilter = (updater: string | ((old: string) => string)) => {
		if (updater instanceof Function) {
			globalFilter = updater(globalFilter);
		} else {
			globalFilter = updater;
		}
		options.update((old) => ({
			...old,
			state: {
				...old.state,
				globalFilter
			}
		}));
	};

	// Create table instance with writable options
	const options = writable({
		get data() {
			return results;
		},
		columns,
		get state() {
			return {
				sorting,
				columnFilters,
				globalFilter
			};
		},
		onSortingChange: setSorting,
		onColumnFiltersChange: setColumnFilters,
		onGlobalFilterChange: setGlobalFilter,
		getCoreRowModel: getCoreRowModel(),
		getSortedRowModel: getSortedRowModel(),
		getFilteredRowModel: getFilteredRowModel(),
		getPaginationRowModel: getPaginationRowModel(),
		initialState: {
			pagination: {
				pageSize: 50
			}
		}
	});

	const table = createSvelteTable(options);

	// Fetch on mount and periodically
	$effect(() => {
		fetchJobs();
		const interval = setInterval(fetchJobs, 5000);
		return () => clearInterval(interval);
	});
</script>

<div class="space-y-6">
	<!-- Job Selection Sidebar and Results -->
	<div class="grid grid-cols-12 gap-6">
		<!-- Job List Sidebar -->
		<div class="col-span-3 space-y-4">
			<div class="border border-[var(--border)] bg-[var(--surface)] p-4">
				<div class="mb-4 flex items-center justify-between">
					<h3 class="text-sm font-semibold tracking-wider text-[var(--fg)] uppercase">
						Completed Jobs
					</h3>
					<button
						onclick={fetchJobs}
						class="text-xs text-[var(--text-muted)] hover:text-[var(--accent)]"
					>
						↻
					</button>
				</div>

				{#if loading}
					<div class="py-8 text-center">
						<div
							class="inline-block h-4 w-4 animate-spin border-2 border-[var(--border)] border-t-[var(--accent)]"
						></div>
					</div>
				{:else if jobs.length === 0}
					<div class="py-8 text-center">
						<p class="text-xs text-[var(--text-muted)]">No completed jobs yet.</p>
					</div>
				{:else}
					<div class="space-y-2">
						{#each jobs as job (job.id)}
							<button
								onclick={() => selectJob(job.id)}
								class="w-full border border-[var(--border)] p-3 text-left transition-colors {selectedJobId ===
								job.id
									? 'border-[var(--accent)] bg-[var(--bg)]'
									: 'bg-[var(--surface)] hover:border-[var(--accent)]'}"
							>
								<div class="mb-1 font-mono text-xs text-[var(--fg)]">{job.id.slice(0, 8)}</div>
								<div class="text-xs text-[var(--text-muted)]">{job.executor}</div>
								<div class="mt-1 text-xs text-[var(--text-muted)]">
									{formatRelativeTime(job.completed_at || job.created_at)}
								</div>
							</button>
						{/each}
					</div>
				{/if}
			</div>
		</div>

		<!-- Results Panel -->
		<div class="col-span-9 space-y-6">
			{#if !selectedJobId}
				<div
					class="flex min-h-[400px] items-center justify-center border border-[var(--border)] bg-[var(--surface)] p-16 text-center"
				>
					<div class="text-center">
						<p class="text-sm text-[var(--text-muted)]">Select a completed job to view results</p>
					</div>
				</div>
			{:else if loadingResults}
				<div
					class="flex min-h-[400px] items-center justify-center border border-[var(--border)] bg-[var(--surface)] p-16 text-center"
				>
					<div class="text-center">
						<div
							class="inline-block h-8 w-8 animate-spin border-2 border-[var(--border)] border-t-[var(--accent)]"
						></div>
						<p class="mt-4 text-sm text-[var(--text-muted)]">Loading results...</p>
					</div>
				</div>
			{:else if results.length === 0}
				<div
					class="flex min-h-[400px] items-center justify-center border border-[var(--border)] bg-[var(--surface)] p-16 text-center"
				>
					<div class="text-center">
						<p class="text-sm text-[var(--text-muted)]">No results found for this job.</p>
					</div>
				</div>
			{:else}
				<!-- Header with Search -->
				<div class="border border-[var(--border)] bg-[var(--surface)] p-6">
					<div class="mb-4 flex items-center justify-between">
						<h2 class="text-lg font-semibold text-[var(--fg)]">
							Results for {selectedJobId.slice(0, 8)}
						</h2>
					</div>

					<!-- Search -->
					<div>
						<input
							type="text"
							bind:value={searchInput}
							oninput={handleSearchInput}
							placeholder="Search all columns..."
							class="w-full border border-[var(--border)] bg-[var(--bg)] px-4 py-2 text-sm text-[var(--fg)] placeholder-[var(--text-muted)] focus:border-[var(--accent)] focus:outline-none"
						/>
					</div>
				</div>
				<!-- Stats -->
				<div
					class="border border-[var(--border)] bg-[var(--surface)] p-6 transition-opacity duration-200"
				>
					<div class="grid grid-cols-3 gap-8">
						<div>
							<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
								Total Results
							</div>
							<div class="text-3xl font-semibold text-[var(--fg)] tabular-nums">
								{results.length}
							</div>
						</div>
						<div>
							<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
								Filtered
							</div>
							<div class="text-3xl font-semibold text-[var(--fg)] tabular-nums">
								{$table.getFilteredRowModel().rows.length}
							</div>
						</div>
						<div>
							<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
								Unique Keys
							</div>
							<div class="text-3xl font-semibold text-[var(--fg)] tabular-nums">
								{new Set(results.map((r) => r.key)).size}
							</div>
						</div>
					</div>
				</div>

				<!-- Table -->
				<div class="border border-[var(--border)] bg-[var(--surface)]">
					<div class="overflow-x-auto">
						<table class="w-full text-sm">
							<thead class="border-b border-[var(--border)] bg-[var(--bg)]">
								{#each $table.getHeaderGroups() as headerGroup (headerGroup.id)}
									<tr>
										{#each headerGroup.headers as header (header.id)}
											<th class="px-4 py-3 text-left">
												{#if !header.isPlaceholder}
													<button
														class="flex items-center gap-2 font-semibold tracking-wider text-[var(--text-muted)] uppercase hover:text-[var(--fg)]"
														onclick={header.column.getToggleSortingHandler()}
													>
														{header.column.columnDef.header}
														{#if header.column.getIsSorted()}
															{@const sortDir = header.column.getIsSorted()}
															<span class="text-[var(--accent)]">
																{sortDir === 'asc' ? '↑' : sortDir === 'desc' ? '↓' : ''}
															</span>
														{/if}
													</button>
												{/if}
											</th>
										{/each}
									</tr>
								{/each}
							</thead>
							<tbody>
								{#each $table.getRowModel().rows as row (row.id)}
									<tr class="border-b border-[var(--border)] hover:bg-[var(--bg)]">
										{#each row.getVisibleCells() as cell (cell.id)}
											<td class="px-4 py-3 font-mono text-[var(--fg)]">
												{cell.getValue()}
											</td>
										{/each}
									</tr>
								{/each}
							</tbody>
						</table>
					</div>

					<!-- Pagination -->
					<div
						class="flex items-center justify-between border-t border-[var(--border)] bg-[var(--bg)] px-4 py-3"
					>
						<div class="flex items-center gap-2">
							<button
								onclick={() => $table.setPageIndex(0)}
								disabled={!$table.getCanPreviousPage()}
								class="border border-[var(--border)] bg-[var(--surface)] px-3 py-1 text-xs text-[var(--fg)] transition-colors hover:border-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-30"
							>
								First
							</button>
							<button
								onclick={() => $table.previousPage()}
								disabled={!$table.getCanPreviousPage()}
								class="border border-[var(--border)] bg-[var(--surface)] px-3 py-1 text-xs text-[var(--fg)] transition-colors hover:border-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-30"
							>
								Previous
							</button>
							<button
								onclick={() => $table.nextPage()}
								disabled={!$table.getCanNextPage()}
								class="border border-[var(--border)] bg-[var(--surface)] px-3 py-1 text-xs text-[var(--fg)] transition-colors hover:border-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-30"
							>
								Next
							</button>
							<button
								onclick={() => $table.setPageIndex($table.getPageCount() - 1)}
								disabled={!$table.getCanNextPage()}
								class="border border-[var(--border)] bg-[var(--surface)] px-3 py-1 text-xs text-[var(--fg)] transition-colors hover:border-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-30"
							>
								Last
							</button>
						</div>

						<div class="flex items-center gap-4">
							<span class="text-xs text-[var(--text-muted)]">
								Page {$table.getState().pagination.pageIndex + 1} of {$table.getPageCount()}
							</span>
							<span class="text-xs text-[var(--text-muted)]">|</span>
							<span class="text-xs text-[var(--text-muted)]">
								{$table.getFilteredRowModel().rows.length} rows
							</span>
							<select
								bind:value={pageSize}
								onchange={(e) => $table.setPageSize(Number(e.currentTarget.value))}
								class="border border-[var(--border)] bg-[var(--surface)] px-3 py-1 pr-6 text-xs text-[var(--fg)]"
							>
								{#each [10, 25, 50, 100, 200] as pageSizeOption (pageSizeOption)}
									<option value={pageSizeOption}>Show {pageSizeOption}</option>
								{/each}
							</select>
						</div>
					</div>
				</div>
			{/if}
		</div>
	</div>
</div>
