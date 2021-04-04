using System;
using System.Net.Sockets;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using StackExchange.Redis;
using static System.Console;

namespace AzureRedisCache
{
    public static class RedisKey
    {
        public const string Message = "Message";
        public const string EmployeeUser = "EmployeeUser";
    }

    class Program
    {
        static void Main(string[] args)
        {
            InitializeConfiguration();

            IDatabase cache = GetDatabase();

            string cacheCommand = "PING";
            WriteLine("\nCache command : " + cacheCommand);
            WriteLine("Cache response : " + cache.Execute(cacheCommand).ToString());

            // Simple get and put of integral data types into the cache
            cacheCommand = "GET Message";
            WriteLine("\nCache command  : " + cacheCommand + " or StringGet()");
            WriteLine("Cache response : " + cache.StringGet(RedisKey.Message).ToString());

            cacheCommand = "SET Message \"Hello! The cache is working from a .NET Core console app!\"";
            WriteLine("\nCache command  : " + cacheCommand + " or StringSet()");
            WriteLine("Cache response : " + cache.StringSet(RedisKey.Message, "Hello! The cache is working from a .NET Core console app!").ToString());

            cacheCommand = "CLIENT LIST";
            Console.WriteLine("\nCache command  : " + cacheCommand);
            var endpoint = (System.Net.DnsEndPoint)GetEndPoints()[0];
            IServer server = GetServer(endpoint.Host, endpoint.Port);
            ClientInfo[] clients = server.ClientList();

            Console.WriteLine("Cache response :");
            foreach (ClientInfo client in clients)
            {
                Console.WriteLine(client.Raw);
            }

            EmployeeUser e007 = new EmployeeUser(1, "Vincent Llauderes", "United State California.");

            cacheCommand = "Storing EmployeeUser";
            WriteLine("\nCache command : " + cacheCommand);
            cache.StringSet(RedisKey.EmployeeUser, JsonConvert.SerializeObject(e007));

            // Retrieve .NET object from cache
            EmployeeUser e007FromCache = JsonConvert.DeserializeObject<EmployeeUser>(cache.StringGet(RedisKey.EmployeeUser));
            WriteLine("Cache response : " + e007FromCache);
            WriteLine("Deserialized Employee .NET object :\n");
            WriteLine("\tEmployee.Name : " + e007FromCache.Name);
            WriteLine("\tEmployee.Id   : " + e007FromCache.Id);
            WriteLine("\tEmployee.Age  : " + e007FromCache.Address + "\n");

            CloseConnection(lazyConnection);
        }

        class EmployeeUser
        {
            public int Id { get; private set; }
            public string Name { get; private set; }
            public string Address { get; private set; }

            public EmployeeUser(int id, string name, string address)
            {
                Id = id;
                Name = name;
                Address = address;
            }
        }

        // Get stored secret in the project.
        private static IConfigurationRoot Configuration { get; set; }
        const string SecretName = "CacheConnection";

        private static void InitializeConfiguration()
        {
            var builder = new ConfigurationBuilder().AddUserSecrets<Program>();
            Configuration = builder.Build();
        }

        // Initialize Redis connection.
        public static ConnectionMultiplexer Connection
        {
            get { return lazyConnection.Value; }
        }
        private static Lazy<ConnectionMultiplexer> lazyConnection = CreateConnection();

        private static Lazy<ConnectionMultiplexer> CreateConnection()
        {
            return new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(Configuration[SecretName]));
        }

        private static long lastReconnectTicks = DateTimeOffset.MinValue.UtcTicks;
        private static DateTimeOffset firstErrorTime = DateTimeOffset.MinValue;
        private static DateTimeOffset previousErrorTime = DateTimeOffset.MinValue;

        private static readonly object reconnectLock = new object();

        // In general, let StackExchange.Redis handle most reconnects,
        // so limit the frequency of how often ForceReconnect() will
        // actually reconnect.
        public static TimeSpan ReconnectMinFrequency => TimeSpan.FromSeconds(60);

        // If errors continue for longer than the below threshold, then the
        // multiplexer seems to not be reconnecting, so ForceReconnect() will
        // re-create the multiplexer.
        public static TimeSpan ReconnectErrorThreshold => TimeSpan.FromSeconds(30);

        public static int RetryMaxAttempts => 5;

        private static void CloseConnection(Lazy<ConnectionMultiplexer> oldConnection)
        {
            if (oldConnection == null)
                return;

            try
            {
                oldConnection.Value.Close();
            }
            catch (Exception)
            {
                // Example error condition: if accessing oldConnection.Value causes a connection attempt and that fails.
            }
        }

        /// <summary>
        /// Force a new ConnectionMultiplexer to be created.
        /// NOTES:
        ///     1. Users of the ConnectionMultiplexer MUST handle ObjectDisposedExceptions, which can now happen as a result of calling ForceReconnect().
        ///     2. Don't call ForceReconnect for Timeouts, just for RedisConnectionExceptions or SocketExceptions.
        ///     3. Call this method every time you see a connection exception. The code will:
        ///         a. wait to reconnect for at least the "ReconnectErrorThreshold" time of repeated errors before actually reconnecting
        ///         b. not reconnect more frequently than configured in "ReconnectMinFrequency"
        /// </summary>
        public static void ForceReconnect()
        {
            var utcNow = DateTimeOffset.UtcNow;
            long previousTicks = Interlocked.Read(ref lastReconnectTicks);
            var previousReconnectTime = new DateTimeOffset(previousTicks, TimeSpan.Zero);
            TimeSpan elapsedSinceLastReconnect = utcNow - previousReconnectTime;

            // If multiple threads call ForceReconnect at the same time, we only want to honor one of them.
            if (elapsedSinceLastReconnect < ReconnectMinFrequency)
                return;

            lock (reconnectLock)
            {
                utcNow = DateTimeOffset.UtcNow;
                elapsedSinceLastReconnect = utcNow - previousReconnectTime;

                if (firstErrorTime == DateTimeOffset.MinValue)
                {
                    // We haven't seen an error since last reconnect, so set initial values.
                    firstErrorTime = utcNow;
                    previousErrorTime = utcNow;
                    return;
                }

                if (elapsedSinceLastReconnect < ReconnectMinFrequency)
                    return; // Some other thread made it through the check and the lock, so nothing to do.

                TimeSpan elapsedSinceFirstError = utcNow - firstErrorTime;
                TimeSpan elapsedSinceMostRecentError = utcNow - previousErrorTime;

                bool shouldReconnect =
                    elapsedSinceFirstError >= ReconnectErrorThreshold // Make sure we gave the multiplexer enough time to reconnect on its own if it could.
                    && elapsedSinceMostRecentError <= ReconnectErrorThreshold; // Make sure we aren't working on stale data (e.g. if there was a gap in errors, don't reconnect yet).

                // Update the previousErrorTime timestamp to be now (e.g. this reconnect request).
                previousErrorTime = utcNow;

                if (!shouldReconnect)
                    return;

                firstErrorTime = DateTimeOffset.MinValue;
                previousErrorTime = DateTimeOffset.MinValue;

                Lazy<ConnectionMultiplexer> oldConnection = lazyConnection;
                CloseConnection(oldConnection);
                lazyConnection = CreateConnection();
                Interlocked.Exchange(ref lastReconnectTicks, utcNow.UtcTicks);
            }
        }

        // In real applications, consider using a framework such as
        // Polly to make it easier to customize the retry approach.
        private static T BasicRetry<T>(Func<T> func)
        {
            int reconnectRetry = 0;
            int disposedRetry = 0;

            while (true)
            {
                try
                {
                    return func();
                }
                catch (Exception ex) when (ex is RedisConnectionException || ex is SocketException)
                {
                    reconnectRetry++;
                    if (reconnectRetry > RetryMaxAttempts)
                        throw;
                    ForceReconnect();
                }
                catch (ObjectDisposedException)
                {
                    disposedRetry++;
                    if (disposedRetry > RetryMaxAttempts)
                        throw;
                }
            }
        }

        public static IDatabase GetDatabase()
        {
            return BasicRetry(() => Connection.GetDatabase());
        }

        public static System.Net.EndPoint[] GetEndPoints()
        {
            return BasicRetry(() => Connection.GetEndPoints());
        }

        public static IServer GetServer(string host, int port)
        {
            return BasicRetry(() => Connection.GetServer(host, port));
        }
    }
}
