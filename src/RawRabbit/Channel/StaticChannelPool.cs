using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RawRabbit.Common;
using RawRabbit.Exceptions;
using RawRabbit.Logging;

namespace RawRabbit.Channel
{
	public interface IChannelPool
	{
		Task<IModel> GetAsync(CancellationToken ct = default(CancellationToken));
	}

	public class StaticChannelPool : IDisposable, IChannelPool
	{
		protected readonly LinkedList<IModel> Pool;
		protected readonly List<IRecoverable> Recoverables;
		protected readonly ConcurrentChannelQueue ChannelRequestQueue;
		private readonly Task serveChannelsTask;
		private readonly CancellationTokenSource serveChannelsToken = new CancellationTokenSource();
		private readonly AsyncAutoResetEvent channelRecoveredEvent = new AsyncAutoResetEvent(false);
		private readonly ILog _logger = LogProvider.For<StaticChannelPool>();

		public StaticChannelPool(IEnumerable<IModel> seed)
		{
			seed = seed.ToList();
			Pool = new LinkedList<IModel>(seed);
			Recoverables = new List<IRecoverable>();
			ChannelRequestQueue = new ConcurrentChannelQueue();
			serveChannelsTask = Task.Run(() => StartServeChannels(), serveChannelsToken.Token)
				.ContinueWith(task =>
				{
					if(task.IsCanceled == false && task.IsFaulted)
					{
						_logger.Info(task.Exception, "An unhandled exception occurred when serving channels.");
					}
				});
			foreach (var channel in seed)
			{
				ConfigureRecovery(channel);
			}
		}

		private async Task StartServeChannels()
		{
			_logger.Debug("Starting serving channels.");
			for(; ;)
			{
				TaskCompletionSource<IModel> modelTcs = await ChannelRequestQueue.DequeueAsync(serveChannelsToken.Token);

				//Wait for an open channel to serve the channel request
				for(; ; )
				{
					serveChannelsToken.Token.ThrowIfCancellationRequested();

					var currentChannel = Pool.First;
					while (currentChannel != null)
					{
						var nextChannel = currentChannel.Next;
						if (currentChannel.Value.IsClosed)
						{
							Pool.Remove(currentChannel);
						}
						else
						{
							break;
						}
						currentChannel = nextChannel;
					}

					if (currentChannel != null)
					{
						//Channel found
						modelTcs.TrySetResult(currentChannel.Value);
						break;
					}
					else
					{
						if (Recoverables.Count > 0)
						{
							//At least one recoverable channel found -> channel may be served later on
							_logger.Info("No open channels in pool, but {recoveryCount} waiting for recovery", Recoverables.Count);
							await channelRecoveredEvent.WaitAsync(serveChannelsToken.Token);
						}
						else
						{
							//Neither open nor recoverable channels found
							modelTcs.TrySetException(new ChannelAvailabilityException("No open channels in pool and no recoverable channels"));
							break;
						}
					}
				}
			}
		}

		protected virtual int GetActiveChannelCount()
		{
			return Enumerable
				.Concat<object>(Pool, Recoverables)
				.Distinct()
				.Count();
		}

		protected void ConfigureRecovery(IModel channel)
		{
			if (!(channel is IRecoverable recoverable))
			{
				_logger.Debug("Channel {channelNumber} is not recoverable. Recovery disabled for this channel.", channel.ChannelNumber);
				return;
			}
			if (channel.IsClosed && channel.CloseReason != null && channel.CloseReason.Initiator == ShutdownInitiator.Application)
			{
				_logger.Debug("{Channel {channelNumber} is closed by the application. Channel will remain closed and not be part of the channel pool", channel.ChannelNumber);
				return;
			}
			Recoverables.Add(recoverable);
			recoverable.Recovery += (sender, args) =>
			{
				_logger.Info("Channel {channelNumber} has been recovered and will be re-added to the channel pool", channel.ChannelNumber);
				if (Pool.Contains(channel))
				{
					return;
				}
				Pool.AddLast(channel);
				channelRecoveredEvent.Set();
			};
			channel.ModelShutdown += (sender, args) =>
			{
				if (args.Initiator == ShutdownInitiator.Application)
				{
					_logger.Info("Channel {channelNumber} is being closed by the application. No recovery will be performed.", channel.ChannelNumber);
					Recoverables.Remove(recoverable);
				}
			};
		}

		public virtual Task<IModel> GetAsync(CancellationToken ct = default(CancellationToken))
		{
			var channelTcs = new TaskCompletionSource<IModel>();
			ChannelRequestQueue.Enqueue(channelTcs);
			ct.Register(() => channelTcs.TrySetCanceled());
			return channelTcs.Task;
		}

		public virtual void Dispose()
		{
			foreach (var channel in Pool)
			{
				channel?.Dispose();
			}
			foreach (var recoverable in Recoverables)
			{
				(recoverable as IModel)?.Dispose();
			}
			serveChannelsToken.Cancel();
			serveChannelsTask.Wait();
			channelRecoveredEvent.Dispose();
		}
	}
}
