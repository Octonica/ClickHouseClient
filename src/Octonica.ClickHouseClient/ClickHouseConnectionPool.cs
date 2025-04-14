using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient
{
    public class ClickHouseConnectionPool : IDisposable
    {
        private readonly string _connectionString;
        private readonly ConcurrentBag<ClickHouseConnection> _connections = new ConcurrentBag<ClickHouseConnection>();
        private readonly SemaphoreSlim _poolSemaphore;
        public ClickHouseConnectionPool(string connectionString, int maxPoolSize = 100)
        {
            _connectionString = connectionString;
            _poolSemaphore = new SemaphoreSlim(maxPoolSize, maxPoolSize);
        }

        public async Task<ClickHouseConnection> RentAsync()
        {
            await _poolSemaphore.WaitAsync();

            if (_connections.TryTake(out var connection))
            {
                if (connection.State == System.Data.ConnectionState.Open)
                    return connection;
                
                connection.Dispose();
            }

            var newConnection = new ClickHouseConnection(_connectionString);
            await newConnection.OpenAsync();
            return newConnection;
        }

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

        public void Dispose()
        {
            while (_connections.TryTake(out var conn))
                conn.Dispose();

            _poolSemaphore.Dispose();
        }
    }
}