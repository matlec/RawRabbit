using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RawRabbit.Common
{
	//Based on https://devblogs.microsoft.com/pfxteam/building-async-coordination-primitives-part-2-asyncautoresetevent/
	class AsyncAutoResetEvent : IDisposable
	{
		private static readonly Task s_completed = Task.FromResult(true);
		private readonly Queue<Tuple<TaskCompletionSource<bool>, CancellationTokenRegistration>> m_waits = new Queue<Tuple<TaskCompletionSource<bool>, CancellationTokenRegistration>>();
		private bool m_signaled;

		public AsyncAutoResetEvent(bool initialState)
		{
			m_signaled = initialState;
		}

		public Task WaitAsync(CancellationToken ct = default(CancellationToken))
		{
			lock (m_waits)
			{
				if (m_signaled)
				{
					m_signaled = false;
					return s_completed;
				}
				else
				{
					var tcs = new TaskCompletionSource<bool>();
					CancellationTokenRegistration ctRegistration = default(CancellationTokenRegistration);
					if (ct != null)
					{
						ctRegistration = ct.Register(() =>
						{
							tcs.TrySetCanceled();
						});
					}
					m_waits.Enqueue(new Tuple<TaskCompletionSource<bool>, CancellationTokenRegistration>(tcs, ctRegistration));
					return tcs.Task;
				}
			}
		}

		public void Set()
		{
			TaskCompletionSource<bool> toRelease = null;

			lock (m_waits)
			{
				if (m_waits.Count > 0)
				{
					var wait = m_waits.Dequeue();
					toRelease = wait.Item1;
					wait.Item2.Dispose();
				}
				else if (!m_signaled)
				{
					m_signaled = true;
				}
			}

			toRelease?.TrySetResult(true);
		}

		#region IDisposable Support
		private bool disposedValue = false; // To detect redundant calls

		protected virtual void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					lock (m_waits)
					{
						foreach(var wait in m_waits)
						{
							if(wait.Item2 != default(CancellationTokenRegistration))
							{
								wait.Item2.Dispose();
							}
						}
						m_waits.Clear();
					}
				}
				disposedValue = true;
			}
		}

		// This code added to correctly implement the disposable pattern.
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(true);
			GC.SuppressFinalize(this);
		}
		#endregion
	}
}
