using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace NFig.Redis
{
    public class NFigRedisStore<TSettings, TTier, TDataCenter> : NFigAsyncStore<TSettings, TTier, TDataCenter>, IDisposable
        where TSettings : class, INFigSettings<TTier, TDataCenter>, new()
        where TTier : struct
        where TDataCenter : struct
    {
        public delegate void SettingsUpdateDelegate(Exception ex, TSettings settings, NFigRedisStore<TSettings, TTier, TDataCenter> nfigRedisStore);

        private class TierDataCenterCallback
        {
            public TTier Tier { get; }
            public TDataCenter DataCenter { get; }
            public SettingsUpdateDelegate Callback { get; }
            public string LastNotifiedCommit { get; set; } = "NONE";

            public TierDataCenterCallback(TTier tier, TDataCenter dataCenter, SettingsUpdateDelegate callback)
            {
                Tier = tier;
                DataCenter = dataCenter;
                Callback = callback;
            }
        }

        private const string APP_UPDATE_CHANNEL = "NFig-AppUpdate";
        private const string COMMIT_KEY = "$commit";

        private readonly ConnectionMultiplexer _redis;
        private readonly ISubscriber _subscriber;
        private readonly int _dbIndex;

        private Timer _contingencyPollingTimer;

        private readonly object _callbacksLock = new object();
        private readonly Dictionary<string, TierDataCenterCallback[]> _callbacksByApp = new Dictionary<string, TierDataCenterCallback[]>();

        private readonly object _dataCacheLock = new object();
        private readonly Dictionary<string, RedisAppData> _dataCache = new Dictionary<string, RedisAppData>();

        private readonly object _infoCacheLock = new object();
        private readonly Dictionary<string, SettingInfoData> _infoCache = new Dictionary<string, SettingInfoData>();

        public int ContingencyPollingInterval { get; }
        public string RedisKeyPrefix { get; }

        public NFigRedisStore(
            string redisConnectionString, 
            int dbIndex = 0,
            Dictionary<Type, SettingConverterAttribute> additionalDefaultConverters = null,
            int contingencyPollingInterval = 60,
            string redisKeyPrefix = "NFig:"
            )
        : this (
            ConnectionMultiplexer.Connect(redisConnectionString),
            dbIndex,
            additionalDefaultConverters,
            contingencyPollingInterval,
            redisKeyPrefix
        )
        {
        }

        // The reason this constructor is private and there is a public static method wrapper is so the calling dll isn't required to reference to SE.Redis.
        private NFigRedisStore(
            ConnectionMultiplexer redisConnection, 
            int dbIndex,
            Dictionary<Type, SettingConverterAttribute> additionalDefaultConverters,
            int contingencyPollingInterval,
            string redisKeyPrefix
            )
            : base(new SettingsFactory<TSettings, TTier, TDataCenter>(additionalDefaultConverters))
        {
            _redis = redisConnection;
            _subscriber = _redis.GetSubscriber();
            _dbIndex = dbIndex;
            ContingencyPollingInterval = contingencyPollingInterval;
            RedisKeyPrefix = redisKeyPrefix;
        }

        public static NFigRedisStore<TSettings, TTier, TDataCenter> FromConnectionMultiplexer(
            ConnectionMultiplexer redisConnection,
            int db = 0,
            Dictionary<Type, SettingConverterAttribute> additionalDefaultConverters = null,
            int contingencyPollingInterval = 60,
            string redisKeyPrefix = "NFig:"
            )
        {
            return new NFigRedisStore<TSettings, TTier, TDataCenter>(redisConnection, db, additionalDefaultConverters, contingencyPollingInterval, redisKeyPrefix);
        }

        public void SubscribeToAppSettings(string appName, TTier tier, TDataCenter dataCenter, SettingsUpdateDelegate callback)
        {
            TierDataCenterCallback[] callbacks;
            lock (_callbacksLock)
            {
                var info = new TierDataCenterCallback(tier, dataCenter, callback);
                if (_callbacksByApp.TryGetValue(appName, out callbacks))
                {
                    foreach (var c in callbacks)
                    {
                        if (c.Tier.Equals(tier) && c.DataCenter.Equals(dataCenter) && c.Callback == callback)
                            return; // callback already exists, no need to add it again
                    }

                    var oldCallbacks = callbacks;
                    callbacks = new TierDataCenterCallback[oldCallbacks.Length + 1];
                    Array.Copy(oldCallbacks, callbacks, oldCallbacks.Length);
                    callbacks[oldCallbacks.Length] = info;

                    _callbacksByApp[appName] = callbacks;
                }
                else
                {
                    if (_callbacksByApp.Count == 0)
                    {
                        // set up a subscription if this is the first app subscription
                        BeginSubscription();
                    }

                    callbacks = new [] { info };
                    _callbacksByApp[appName] = callbacks;
                }
            }

            ReloadAndNotifyCallback(appName, callbacks);
        }

        /// <summary>
        /// Unsubscribes from app settings updates.
        /// Note that there is a potential race condition if you unsibscribe while an update is in progress, the prior callback may still get called.
        /// </summary>
        /// <param name="appName">The name of the app.</param>
        /// <param name="tier"></param>
        /// <param name="dataCenter"></param>
        /// <param name="callback">(optional) If null, any callback will be removed. If specified, a current callback will only be removed if it is equal to this param.</param>
        /// <returns>The number of callbacks removed.</returns>
        public int UnsubscribeFromAppSettings(string appName, TTier? tier = null, TDataCenter? dataCenter = null, SettingsUpdateDelegate callback = null)
        {
            lock (_callbacksLock)
            {
                var removedCount = 0;
                TierDataCenterCallback[] callbacks;
                if (_callbacksByApp.TryGetValue(appName, out callbacks))
                {
                    var callbackList = new List<TierDataCenterCallback>(callbacks);
                    for (var i = callbackList.Count - 1; i >= 0; i--)
                    {
                        var c = callbackList[i];

                        if ((tier == null || c.Tier.Equals(tier.Value)) && (dataCenter == null || c.DataCenter.Equals(dataCenter.Value)) && (callback == null || c.Callback == callback))
                        {
                            callbackList.RemoveAt(i);
                            removedCount++;
                        }
                    }

                    if (removedCount > 0)
                        _callbacksByApp[appName] = callbackList.ToArray();
                }

                return removedCount;
            }
        }

        public override async Task<TSettings> GetAppSettingsAsync(string appName, TTier tier, TDataCenter dataCenter)
        {
            var data = await GetCurrentDataAsync(appName).ConfigureAwait(false);

            TSettings settings;
            var ex = GetSettingsObjectFromData(data, tier, dataCenter, out settings);
            if (ex != null)
                throw ex;

            return settings;
        }

        public override async Task SetOverrideAsync(string appName, string settingName, string value, TTier tier, TDataCenter dataCenter)
        {
            // make sure this is even valid input before saving it to Redis
            if (!IsValidStringForSetting(settingName, value))
                throw new InvalidSettingValueException<TTier, TDataCenter>(
                    "\"" + value + "\" is not a valid value for setting \"" + settingName + "\"",
                    settingName,
                    value, 
                    false,
                    tier,
                    dataCenter);

            var key = GetSettingKey(settingName, tier, dataCenter);
            var db = GetRedisDb();

            await db.HashSetAsync(GetRedisHashName(appName), new [] { new HashEntry(key, value), new HashEntry(COMMIT_KEY, NewCommit()) }).ConfigureAwait(false);
            await _subscriber.PublishAsync(APP_UPDATE_CHANNEL, appName).ConfigureAwait(false);
        }

        public override async Task ClearOverrideAsync(string appName, string settingName, TTier tier, TDataCenter dataCenter)
        {
            var key = GetSettingKey(settingName, tier, dataCenter);
            var db = GetRedisDb();

            var hashName = GetRedisHashName(appName);
            var tran = db.CreateTransaction();
            var delTask = tran.HashDeleteAsync(hashName, key);
            var setTask = tran.HashSetAsync(hashName, COMMIT_KEY, NewCommit());
            var committed = await tran.ExecuteAsync().ConfigureAwait(false);
            if (!committed)
                throw new NFigException("Unable to clear override. Redis Transaction failed. " + appName + "." + settingName);

            // not sure if these actually need to be awaited after ExecuteAsync finishes
            await delTask.ConfigureAwait(false);
            await setTask.ConfigureAwait(false);

            await _subscriber.PublishAsync(APP_UPDATE_CHANNEL, appName).ConfigureAwait(false);
        }

        public override async Task<string> GetCurrentCommitAsync(string appName)
        {
            var db = GetRedisDb();
            return await db.HashGetAsync(GetRedisHashName(appName), COMMIT_KEY).ConfigureAwait(false);
        }

        public override async Task<SettingInfo<TTier, TDataCenter>[]> GetAllSettingInfosAsync(string appName)
        {
            var data = await GetCurrentDataAsync(appName).ConfigureAwait(false);
            return Factory.GetAllSettingInfos(data.Overrides);
        }

        public override async Task<SettingInfo<TTier, TDataCenter>> GetSettingInfoAsync(string appName, string settingName)
        {
            // todo: should probably call GetAllSettingInfosAsync and have it perform caching rather than redoing work and reproducing logic in this method
            SettingInfoData data;
            // ReSharper disable once InconsistentlySynchronizedField
            if (_infoCache.TryGetValue(appName, out data))
            {
                // check if cached info is valid
                var commit = await GetCurrentCommitAsync(appName).ConfigureAwait(false);
                if (data.Commit == commit)
                    return data.InfoBySetting[settingName];
            }

            data = new SettingInfoData();
            var redisData = await GetCurrentDataAsync(appName).ConfigureAwait(false);
            data.Commit = redisData.Commit;
            data.InfoBySetting = Factory.GetAllSettingInfos(redisData.Overrides).ToDictionary(s => s.Name);

            lock (_infoCacheLock)
            {
                _infoCache[appName] = data;
            }

            return data.InfoBySetting[settingName];
        }

        public async Task CopySettingsFrom(string appName, string redisConnectionString, int dbIndex = 0)
        {
            using (var otherRedis = ConnectionMultiplexer.Connect(redisConnectionString))
                await CopySettings(appName, otherRedis.GetDatabase(dbIndex), GetRedisDb());
        }

        public async Task CopySettingsTo(string appName, string redisConnectionString, int dbIndex = 0)
        {
            using (var otherRedis = ConnectionMultiplexer.Connect(redisConnectionString))
                await CopySettings(appName, GetRedisDb(), otherRedis.GetDatabase(dbIndex));
        }

        private async Task CopySettings(string appName, IDatabaseAsync srcRedis, IDatabaseAsync dstRedis)
        {
            var hashName = GetRedisHashName(appName);
            var serialized = await srcRedis.KeyDumpAsync(hashName).ConfigureAwait(false);
            await dstRedis.KeyRestoreAsync(hashName, serialized).ConfigureAwait(false);
            await _subscriber.PublishAsync(APP_UPDATE_CHANNEL, appName).ConfigureAwait(false);
        }

        // ReSharper disable once StaticMemberInGenericType
        private static readonly Regex s_keyRegex = new Regex(@"^:(?<Tier>\d+):(?<DataCenter>\d+);(?<Name>.+)$");
        private async Task<RedisAppData> GetCurrentDataAsync(string appName)
        {
            RedisAppData data;

            var firstTimeLoad = true;
            // check cache first
            // ReSharper disable once InconsistentlySynchronizedField
            if (_dataCache.TryGetValue(appName, out data))
            {
                firstTimeLoad = false;
                var commit = await GetCurrentCommitAsync(appName).ConfigureAwait(false);
                if (data.Commit == commit)
                    return data;
            }

            var tierType = typeof(TTier);
            var dataCenterType = typeof(TDataCenter);

            data = new RedisAppData();
            data.ApplicationName = appName;

            // grab the redis hash
            var db = GetRedisDb();
            var hash = await db.HashGetAllAsync(GetRedisHashName(appName)).ConfigureAwait(false);

            var overrides = new List<SettingValue<TTier, TDataCenter>>();
            foreach (var hashEntry in hash)
            {
                string key = hashEntry.Name;
                var match = s_keyRegex.Match(key);
                if (match.Success)
                {
                    overrides.Add(new SettingValue<TTier, TDataCenter>(
                        match.Groups["Name"].Value,
                        hashEntry.Value,
                        (TTier)Enum.ToObject(tierType, int.Parse(match.Groups["Tier"].Value)),
                        (TDataCenter)Enum.ToObject(dataCenterType, int.Parse(match.Groups["DataCenter"].Value))
                    ));
                }
                else if (key == COMMIT_KEY)
                {
                    data.Commit = hashEntry.Value;
                }
            }

            data.Overrides = overrides;

            lock (_dataCacheLock)
            {
                _dataCache[appName] = data;
            }

            if (firstTimeLoad)
                DeleteOrphanedOverrides(data);

            return data;
        }

        private void DeleteOrphanedOverrides(RedisAppData data)
        {
            var db = GetRedisDb();
            foreach (var over in data.Overrides)
            {
                if (!Factory.SettingExists(over.Name))
                {
                    var hashName = GetRedisHashName(data.ApplicationName);
                    db.HashDelete(hashName, GetSettingKey(over.Name, over.Tier, over.DataCenter), CommandFlags.DemandMaster | CommandFlags.FireAndForget);
                }
            }
        }

        private InvalidSettingOverridesException<TTier, TDataCenter> GetSettingsObjectFromData(RedisAppData data, TTier tier, TDataCenter dataCenter, out TSettings settings)
        {
            // create new settings object
            var ex = Factory.TryGetAppSettings(out settings, tier, dataCenter, data.Overrides);
            settings.ApplicationName = data.ApplicationName;
            settings.Commit = data.Commit;

            return ex;
        }

        private void BeginSubscription()
        {
            _subscriber.Subscribe(APP_UPDATE_CHANNEL, OnAppUpdate);

            // also start polling for changes in case pub/sub fails
            _contingencyPollingTimer = new Timer(PollForChanges, null, ContingencyPollingInterval * 1000, ContingencyPollingInterval * 1000);
        }

        private void PollForChanges(object _)
        {
            List<string> appNames;
            lock (_callbacksLock)
            {
                appNames = new List<string>(_callbacksByApp.Count);
                foreach (var name in _callbacksByApp.Keys)
                {
                    appNames.Add(name);
                }
            }

            foreach (var name in appNames)
            {
                var commit = GetCurrentCommit(name);

                var notify = true;
                RedisAppData data;
                if (_dataCache.TryGetValue(name, out data))
                {
                    notify = data.Commit != commit;
                }

                if (notify)
                {
                    ReloadAndNotifyCallback(name, GetCallbacks(name));
                }
            }
        }

        private void OnAppUpdate(RedisChannel channel, RedisValue message)
        {
            if (channel == APP_UPDATE_CHANNEL)
            {
                ReloadAndNotifyCallback(message, GetCallbacks(message));
            }
        }

        private void ReloadAndNotifyCallback(string appName, TierDataCenterCallback[] callbacks)
        {
            if (callbacks == null || callbacks.Length == 0)
                return;

            Exception ex = null;
            RedisAppData data = null;
            try
            {
                data = Task.Run(async () => await GetCurrentDataAsync(appName) ).Result;
            }
            catch(Exception e)
            {
                ex = e;
            }

            foreach (var c in callbacks)
            {
                if (c.Callback == null)
                    continue;

                if (data != null && data.Commit == c.LastNotifiedCommit)
                    continue;

                TSettings settings = null;
                Exception inner = null;
                if (ex == null)
                {
                    try
                    {
                        ex = GetSettingsObjectFromData(data, c.Tier, c.DataCenter, out settings);
                        c.LastNotifiedCommit = data.Commit;
                    }
                    catch (Exception e)
                    {
                        inner = e;
                    }
                }

                c.Callback(ex ?? inner, settings, this);
            }
        }

        private TierDataCenterCallback[] GetCallbacks(string appName)
        {
            TierDataCenterCallback[] callbacks;
            if (_callbacksByApp.TryGetValue(appName, out callbacks))
                return callbacks;

            return new TierDataCenterCallback[0];
        }

        private static string GetSettingKey(string settingName, TTier tier, TDataCenter dataCenter)
        {
            return ":" + Convert.ToUInt32(tier) + ":" + Convert.ToUInt32(dataCenter) + ";" + settingName;
        }

        private IDatabase GetRedisDb()
        {
            return _redis.GetDatabase(_dbIndex);
        }

        private string GetRedisHashName(string appName)
        {
            return RedisKeyPrefix + appName;
        }

        private class RedisAppData
        {
            public string ApplicationName { get; set; }
            public string Commit { get; set; }
            public IList<SettingValue<TTier, TDataCenter>> Overrides { get; set; }
        }

        private class SettingInfoData
        {
            public string Commit { get; set; }
            public Dictionary<string, SettingInfo<TTier, TDataCenter>> InfoBySetting { get; set; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private bool _disposed;
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                _redis.Dispose();

                var t = _contingencyPollingTimer;
                t?.Dispose();
            }

            _disposed = true;
        }
    }
}
