﻿using System.Threading.Tasks;
using Autofac;
using RawRabbit.Common;
using RawRabbit.DependencyInjection.Autofac;
using RawRabbit.Instantiation;
using RawRabbit.Operations.StateMachine;
using Xunit;

namespace RawRabbit.IntegrationTests.DependecyInjection
{
	public class AutofacTests
	{
		[Fact]
		public async Task Should_Be_Able_To_Resolve_Client_From_Autofac()
		{
			/* Setup */
			var builder = new ContainerBuilder();
			builder.RegisterRawRabbit();
			var container = builder.Build();

			/* Test */
			var client = container.Resolve<IBusClient>();
			var disposer = container.Resolve<IResourceDisposer>();

			/* Assert */
			disposer.Dispose();
			Assert.True(true);
		}

		[Fact]
		public async Task Should_Be_Able_To_Resolve_Client_With_Plugins_From_Autofac()
		{
			/* Setup */
			var builder = new ContainerBuilder();
			builder.RegisterRawRabbit(new RawRabbitOptions
			{
				Plugins = p => p.UseStateMachine()
			});
			var container = builder.Build();

			/* Test */
			var client = container.Resolve<IBusClient>();
			var disposer = container.Resolve<IResourceDisposer>();

			/* Assert */
			disposer.Dispose();
			Assert.True(true);
		}
	}
}