using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// A connection pool for ClickHouse connections, you should use the using keyword declaration,and it's best to use singletons at the application level
    /// </summary>
    public class ClickHouseConnectionPool : IDisposable
    {
        private readonly ClickHouseConnectionStringBuilder? _connectionStringBuilder;
        private readonly ClickHouseConnectionSettings? _connectionSettings;
        private readonly ConcurrentBag<ClickHouseConnection> _connections = new ConcurrentBag<ClickHouseConnection>();
        private readonly SemaphoreSlim _poolSemaphore;
        /// <summary>
        /// Create a connection pool
        /// </summary>
        /// <param name="connectionString">The connection string</param>
        /// <param name="maxPoolSize">Maximum concurrency</param>
        public ClickHouseConnectionPool(string connectionString, int maxPoolSize = 100): this(maxPoolSize)
        {
            _connectionStringBuilder = new ClickHouseConnectionStringBuilder(connectionString);
        }
        
        /// <summary>
        /// Create a connection pool
        /// </summary>
        /// <param name="connectionStringBuilder">The connection string builder</param>
        /// <param name="maxPoolSize">Maximum concurrency</param>
        public ClickHouseConnectionPool(ClickHouseConnectionStringBuilder connectionStringBuilder, int maxPoolSize = 100): this(maxPoolSize)
        {
            _connectionStringBuilder = connectionStringBuilder;
        }
        
        /// <summary>
        /// Create a connection pool
        /// </summary>
        /// <param name="connectionSettings">The connection settings.</param>
        /// <param name="maxPoolSize">Maximum concurrency</param>
        public ClickHouseConnectionPool(ClickHouseConnectionSettings connectionSettings, int maxPoolSize = 100) : this(maxPoolSize)
        {
            _connectionSettings = connectionSettings;
        }

        private ClickHouseConnectionPool(int maxPoolSize)
        {
            _poolSemaphore = new SemaphoreSlim(maxPoolSize, maxPoolSize);
        }

        /// <summary>
        /// Rent out a connection object
        /// </summary>
        /// <returns></returns>
        public async Task<ClickHouseConnection> RentAsync()
        {
            if (_connectionStringBuilder == null && _connectionSettings == null)
                throw new ArgumentNullException($"You should use for connection pool ClickHouseConnectionStringBuilder or ClickHouseConnectionSettings specify the database connection");
            
            await _poolSemaphore.WaitAsync();

            if (_connections.TryTake(out var connection))
            {
                if (connection.State == System.Data.ConnectionState.Open)
                    return connection;
                
                connection.Dispose();
            }

            var newConnection = _connectionStringBuilder != null ? new ClickHouseConnection(_connectionStringBuilder) : new ClickHouseConnection(_connectionSettings!);
            await newConnection.OpenAsync();
            return newConnection;
        }

        /// <summary>
        /// Return a connection object to the pool
        /// </summary>
        /// <param name="connection">The connection object is no longer used</param>
        public void Return(ClickHouseConnection connection)
        {
            if (connection == null)
                return;
           
            if (connection.State == System.Data.ConnectionState.Open)
                _connections.Add(connection);
            else
                connection.Dispose();
        
            _poolSemaphore.Release();
        }

        /// <summary>
        /// Release the connection pool and semaphore
        /// </summary>
        public void Dispose()
        {
            while (_connections.TryTake(out var conn))
                conn.Dispose();

            _poolSemaphore.Dispose();
        }
    }
}