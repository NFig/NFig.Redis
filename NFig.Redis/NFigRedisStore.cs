using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace NFig.Redis
{
    public class NFigRedisStore<TSettings, TTier, TDataCenter> : NFigStore<TSettings, TTier, TDataCenter>
        where TSettings : class, INFigSettings<TTier, TDataCenter>, new()
        where TTier : struct
        where TDataCenter : struct
    {
        private const string APP_UPDATE_CHANNEL = "NFig-AppUpdate";
        private const string COMMIT_KEY = "$commit";

        private readonly ConnectionMultiplexer _redis;
        private readonly ISubscriber _subscriber;
        private readonly int _dbIndex;
        
        public string RedisKeyPrefix { get; }

        public NFigRedisStore(
            string redisConnectionString,
            int dbIndex = 0,
            Dictionary<Type, object> additionalDefaultConverters = null,
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
            Dictionary<Type, object> additionalDefaultConverters,
            int contingencyPollingInterval,
            string redisKeyPrefix
            )
            : base(additionalDefaultConverters, contingencyPollingInterval)
        {
            _redis = redisConnection;
            _subscriber = _redis.GetSubscriber();
            _dbIndex = dbIndex;
            RedisKeyPrefix = redisKeyPrefix;
            
            _subscriber.Subscribe(APP_UPDATE_CHANNEL, OnAppUpdate);
        }

        public static NFigRedisStore<TSettings, TTier, TDataCenter> FromConnectionMultiplexer(
            ConnectionMultiplexer redisConnection,
            int db = 0,
            Dictionary<Type, object> additionalDefaultConverters = null,
            int contingencyPollingInterval = 60,
            string redisKeyPrefix = "NFig:"
            )
        {
            return new NFigRedisStore<TSettings, TTier, TDataCenter>(redisConnection, db, additionalDefaultConverters, contingencyPollingInterval, redisKeyPrefix);
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

            var key = GetOverrideKey(settingName, tier, dataCenter);
            var db = GetRedisDb();

            await db.HashSetAsync(GetRedisHashName(appName), new [] { new HashEntry(key, value), new HashEntry(COMMIT_KEY, NewCommit()) }).ConfigureAwait(false);
            await _subscriber.PublishAsync(APP_UPDATE_CHANNEL, appName).ConfigureAwait(false);
        }

        public override async Task ClearOverrideAsync(string appName, string settingName, TTier tier, TDataCenter dataCenter)
        {
            var key = GetOverrideKey(settingName, tier, dataCenter);
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

        public override string GetCurrentCommit(string appName)
        {
            var db = GetRedisDb();
            return db.HashGet(GetRedisHashName(appName), COMMIT_KEY);
        }

        public async Task CopySettingsFrom(string appName, string redisConnectionString, int dbIndex = 0)
        {
            using (var otherRedis = ConnectionMultiplexer.Connect(redisConnectionString))
                await CopySettings(appName, otherRedis.GetDatabase(dbIndex), GetRedisDb(), _subscriber).ConfigureAwait(false);
        }

        public async Task CopySettingsTo(string appName, string redisConnectionString, int dbIndex = 0)
        {
            using (var otherRedis = ConnectionMultiplexer.Connect(redisConnectionString))
                await CopySettings(appName, GetRedisDb(), otherRedis.GetDatabase(dbIndex), otherRedis.GetSubscriber()).ConfigureAwait(false);
        }

        private async Task CopySettings(string appName, IDatabaseAsync srcRedis, IDatabase dstRedis, ISubscriber subscriber)
        {
            var hashName = GetRedisHashName(appName);
            var serialized = await srcRedis.KeyDumpAsync(hashName).ConfigureAwait(false);

            var tran = dstRedis.CreateTransaction();
#pragma warning disable 4014
            tran.KeyDeleteAsync(hashName);
            tran.KeyRestoreAsync(hashName, serialized);
#pragma warning restore 4014
            await tran.ExecuteAsync(CommandFlags.DemandMaster).ConfigureAwait(false);

            await subscriber.PublishAsync(APP_UPDATE_CHANNEL, appName).ConfigureAwait(false);
        }

        protected override async Task<AppData> GetAppDataNoCacheAsync(string appName)
        {
            var db = GetRedisDb();
            var hash = await db.HashGetAllAsync(GetRedisHashName(appName)).ConfigureAwait(false);
            return HashToAppData(appName, hash);
        }

        protected override AppData GetAppDataNoCache(string appName)
        {
            var db = GetRedisDb();
            var hash = db.HashGetAll(GetRedisHashName(appName));
            return HashToAppData(appName, hash);
        }

        private static AppData HashToAppData(string appName, HashEntry[] hash)
        {
            var data = new AppData();
            data.ApplicationName = appName;

            var overrides = new List<SettingValue<TTier, TDataCenter>>();
            foreach (var hashEntry in hash)
            {
                string key = hashEntry.Name;

                SettingValue<TTier, TDataCenter> value;
                if (TryGetValueFromOverride(key, hashEntry.Value, out value))
                {
                    overrides.Add(value);
                }
                else if (key == COMMIT_KEY)
                {
                    data.Commit = hashEntry.Value;
                }
            }

            data.Overrides = overrides;

            return data;
        }

        protected override Task DeleteOrphanedOverridesAsync(AppData data)
        {
            DeleteOrphanedOverrides(data);
            return Task.FromResult(0);
        }

        protected override void DeleteOrphanedOverrides(AppData data)
        {
            var db = GetRedisDb();
            foreach (var over in data.Overrides)
            {
                if (!SettingExists(over.Name))
                {
                    var hashName = GetRedisHashName(data.ApplicationName);
                    db.HashDelete(hashName, GetOverrideKey(over.Name, over.Tier, over.DataCenter), CommandFlags.DemandMaster | CommandFlags.FireAndForget);
                }
            }
        }

        private void OnAppUpdate(RedisChannel channel, RedisValue appName)
        {
            if (channel == APP_UPDATE_CHANNEL)
            {
                TriggerUpdate(appName);
            }
        }

        private IDatabase GetRedisDb()
        {
            return _redis.GetDatabase(_dbIndex);
        }

        private string GetRedisHashName(string appName)
        {
            return RedisKeyPrefix + appName;
        }
    }
}
