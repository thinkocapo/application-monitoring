using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aspnetcore
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                    webBuilder.UseSentry(o => {
                        o.Dsn = Environment.GetEnvironmentVariable("ASPNETCORE_APP_DSN");
                        o.Release = Environment.GetEnvironmentVariable("RELEASE");
                        o.Environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENV");
                        o.TracesSampleRate = 1.0; 
                    });
                });
    }
}
