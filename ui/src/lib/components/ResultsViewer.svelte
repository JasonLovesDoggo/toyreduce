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

	interface ResultRow {
		key: string;
		value: string;
	}

	let { API_BASE = '' } = $props<{ API_BASE: string }>();

	let results = $state<ResultRow[]>([]);
	let loading = $state(true);
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

	// Fetch all results from master API
	async function fetchResults() {
		loading = true;
		try {
			const res = await fetch(`${API_BASE}/api/results`);
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
			loading = false;
		}
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
		fetchResults();
		const interval = setInterval(fetchResults, 5000);
		return () => clearInterval(interval);
	});
</script>

<div class="space-y-6">
	<!-- Header with Search -->
	<div class="border border-[var(--border)] bg-[var(--surface)] p-6">
		<div class="mb-4 flex items-center justify-between">
			<h2 class="text-lg font-semibold text-[var(--fg)]">All Results</h2>
			<button
				onclick={fetchResults}
				class="border border-[var(--border)] bg-[var(--surface)] px-4 py-2 text-xs tracking-wider text-[var(--fg)] uppercase transition-colors hover:border-[var(--accent)]"
			>
				Refresh
			</button>
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

	{#if loading}
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
				<p class="text-sm text-[var(--text-muted)]">No results available yet.</p>
				<p class="mt-2 text-xs text-[var(--text-muted)]">
					Submit and complete a job to see results here.
				</p>
			</div>
		</div>
	{:else}
		<!-- Stats -->
		<div
			class="border border-[var(--border)] bg-[var(--surface)] p-6 transition-opacity duration-200"
		>
			<div class="grid grid-cols-3 gap-8">
				<div>
					<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">
						Total Results
					</div>
					<div class="text-3xl font-semibold text-[var(--fg)] tabular-nums">{results.length}</div>
				</div>
				<div>
					<div class="mb-2 text-xs tracking-wider text-[var(--text-muted)] uppercase">Filtered</div>
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
