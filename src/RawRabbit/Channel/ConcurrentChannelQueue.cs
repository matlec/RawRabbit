using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RawRabbit.Channel
{
	public class ConcurrentChannelQueue
	{
		private readonly ConcurrentQueue<TaskCompletionSource<IModel>> _queue;
		private readonly SemaphoreSlim _semaphore;

		public ConcurrentChannelQueue()
		{
			_queue = new ConcurrentQueue<TaskCompletionSource<IModel>>();
			_semaphore = new SemaphoreSlim(0);
		}

		public void Enqueue(TaskCompletionSource<IModel> modelTsc)
		{
			_queue.Enqueue(modelTsc);
			_semaphore.Release();
		}

		public async Task<TaskCompletionSource<IModel>> DequeueAsync(CancellationToken ct = default(CancellationToken))
		{
			for(; ; )
			{
				await _semaphore.WaitAsync(ct);
				TaskCompletionSource<IModel> modelTsc;
				if(_queue.TryDequeue(out modelTsc))
				{
					return modelTsc;
				}
			}
		}

		public bool IsEmpty => _queue.IsEmpty;

		public int Count => _queue.Count;
	}
}
