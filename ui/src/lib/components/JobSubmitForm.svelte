<script lang="ts">
	interface Props {
		onSubmit: (data: {
			executor: string;
			input_path: string;
			chunk_size: number;
			reduce_tasks: number;
		}) => void;
		executors: string[];
	}

	let { onSubmit, executors }: Props = $props();

	let executor = $state(executors[0]);
	let inputPath = $state('');
	let chunkSize = $state(16); // Default 16MB
	let reduceTasks = $state(4);
	let submitting = $state(false);

	function handleSubmit(e: Event) {
		e.preventDefault();
		if (submitting) return;

		submitting = true;
		onSubmit({
			executor,
			input_path: inputPath,
			chunk_size: chunkSize,
			reduce_tasks: reduceTasks
		});

		// Reset form after a delay
		setTimeout(() => {
			submitting = false;
			inputPath = '';
		}, 1000);
	}
</script>

<div class="mb-8 border border-[var(--border)] bg-[var(--surface)] p-8">
	<h2 class="mb-6 text-lg font-semibold text-[var(--fg)]">Submit Job</h2>

	<form onsubmit={handleSubmit} class="space-y-6">
		<div class="grid grid-cols-2 gap-6">
			<!-- Executor -->
			<div>
				<label
					for="executor"
					class="mb-2 block text-xs tracking-wider text-[var(--text-muted)] uppercase"
				>
					Executor
				</label>
				<select
					id="executor"
					bind:value={executor}
					class="w-full border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-[var(--fg)] focus:border-[var(--accent)] focus:outline-none"
					required
				>
					{#each executors as exec}
						<option value={exec}>{exec}</option>
					{/each}
				</select>
			</div>

			<!-- Input Path -->
			<div>
				<label
					for="inputPath"
					class="mb-2 block text-xs tracking-wider text-[var(--text-muted)] uppercase"
				>
					Input Path <span class="text-center text-xs">(on control node)</span>
				</label>
				<input
					id="inputPath"
					type="text"
					bind:value={inputPath}
					placeholder="/path/to/input.txt"
					class="w-full border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-[var(--fg)] placeholder-[var(--text-muted)] focus:border-[var(--accent)] focus:outline-none"
					required
				/>
			</div>

			<!-- Chunk Size -->
			<div>
				<label
					for="chunkSize"
					class="mb-2 block text-xs tracking-wider text-[var(--text-muted)] uppercase"
				>
					Chunk Size (MB)
				</label>
				<input
					id="chunkSize"
					type="number"
					bind:value={chunkSize}
					min="1"
					placeholder="16"
					class="w-full border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-[var(--fg)] tabular-nums focus:border-[var(--accent)] focus:outline-none"
					required
				/>
			</div>

			<!-- Reduce Tasks -->
			<div>
				<label
					for="reduceTasks"
					class="mb-2 block text-xs tracking-wider text-[var(--text-muted)] uppercase"
				>
					Reduce Tasks
				</label>
				<input
					id="reduceTasks"
					type="number"
					bind:value={reduceTasks}
					min="1"
					max="16"
					class="w-full border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-[var(--fg)] tabular-nums focus:border-[var(--accent)] focus:outline-none"
					required
				/>
			</div>
		</div>

		<!-- Submit Button -->
		<button
			type="submit"
			disabled={submitting}
			class="bg-[var(--fg)] px-6 py-2 text-[var(--bg)] transition-opacity hover:opacity-80 disabled:cursor-not-allowed disabled:opacity-30"
		>
			{submitting ? 'Submitting...' : 'Submit'}
		</button>
	</form>
</div>
