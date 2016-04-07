using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace NFig.Redis
{
    public class NFigRedisStore<TSettings, TTier, TDataCenter> : NFigStore<TSettings, TTier, TDataCenter>, IDisposable
        where TSettings : class, INFigSettings<TTier, TDataCenter>, new()
        where TTier : struct
        where TDataCenter : struct
    {
        private const string APP_UPDATE_CHANNEL = "NFig-AppUpdate";
        private const string COMMIT_KEY = "$commit";

        private readonly ConnectionMultiplexer _redis;
        private readonly ISubscriber _subscriber;
        private readonly int _dbIndex;
        readonly LoadedLuaScript _setWithCommitScript;

        public string RedisKeyPrefix { get; }

        public NFigRedisStore(
            TTier tier,
            string redisConnectionString,
            int dbIndex = 0,
            Dictionary<Type, object> additionalDefaultConverters = null,
            int contingencyPollingInterval = 60,
            string redisKeyPrefix = "NFig:"
            )
        : this(
            tier,
            ConnectionMultiplexer.Connect(redisConnectionString),
            dbIndex,
            additionalDefaultConverters,
            contingencyPollingInterval,
            redisKeyPrefix
        )
        {
        }

        public void Dispose()
        {
            _redis?.Dispose();
        }

        // The reason this constructor is private and there is a public static method wrapper is so the calling dll isn't required to reference to SE.Redis.
        private NFigRedisStore(
            TTier tier,
            ConnectionMultiplexer redisConnection,
            int dbIndex,
            Dictionary<Type, object> additionalDefaultConverters,
            int contingencyPollingInterval,
            string redisKeyPrefix
            )
            : base(tier, additionalDefaultConverters, contingencyPollingInterval)
        {
            _redis = redisConnection;
            _subscriber = _redis.GetSubscriber();
            _dbIndex = dbIndex;
            RedisKeyPrefix = redisKeyPrefix;

            _subscriber.Subscribe(APP_UPDATE_CHANNEL, OnAppUpdate);
        }

        public static NFigRedisStore<TSettings, TTier, TDataCenter> FromConnectionMultiplexer(
            TTier tier,
            ConnectionMultiplexer redisConnection,
            int db = 0,
            Dictionary<Type, object> additionalDefaultConverters = null,
            int contingencyPollingInterval = 60,
            string redisKeyPrefix = "NFig:"
            )
        {
            return new NFigRedisStore<TSettings, TTier, TDataCenter>(tier, redisConnection, db, additionalDefaultConverters, contingencyPollingInterval, redisKeyPrefix);
        }


        private const string SET_WITH_COMMIT_BODY = @"
if redis.call('hget', @hashName, @commitKey) == @commitId
then
    redis.call('hmset', @hashName, @key, @value, @commitKey, @newCommit)
else
    return redis.error_reply('Commit id mismatch')
end
";


        private static readonly LuaScript s_setWithCommitScript = LuaScript.Prepare(SET_WITH_COMMIT_BODY);


        public override async Task SetOverrideAsync(string appName, string settingName, string value, TDataCenter dataCenter, string commitId = null)
        {
            // make sure this is even valid input before saving it to Redis
            if (!IsValidStringForSetting(settingName, value))
                throw new InvalidSettingValueException<TTier, TDataCenter>(
                    "\"" + value + "\" is not a valid value for setting \"" + settingName + "\"",
                    settingName,
                    value,
                    false,
                    Tier,
                    dataCenter);

            var key = GetOverrideKey(settingName, Tier, dataCenter);
            var db = GetRedisDb();
            if (commitId != null)
            {
                try
                {
                    await db.ScriptEvaluateAsync(s_setWithCommitScript, new
                    {
                        hashName = (RedisKey)GetRedisHashName(appName),
                        commitKey = COMMIT_KEY,
                        commitId,
                        key,
                        value,
                        newCommit = NewCommit()
                    });
                }
                catch (RedisException ex)
                {
                    throw new NFigException("Unable to set override. Redis operation failed. " + appName + "." + settingName, ex);
                }
            }
            else
            {
                await db.HashSetAsync(GetRedisHashName(appName), new[] { new HashEntry(key, value), new HashEntry(COMMIT_KEY, NewCommit()) }).ConfigureAwait(false);
            }

            await _subscriber.PublishAsync(APP_UPDATE_CHANNEL, appName).ConfigureAwait(false);
        }


        private const string CLEAR_WITH_COMMIT_BODY = @"
if redis.call('hget', @hashName, @commitKey) == @commitId
then
    redis.call('hdel', @hashName, @key)
    redis.call('hset', @hashName, @commitKey, @newCommit)
else
    return redis.error_reply('Commit id mismatch')
end
";

        private const string CLEAR_WITHOUT_COMMIT_BODY = @"
redis.call('hdel', @hashName, @key)
redis.call('hset', @hashName, @commitKey, @newCommit)
";

        private static readonly LuaScript s_clearWithCommitCheckScript = LuaScript.Prepare(CLEAR_WITH_COMMIT_BODY);

        private static readonly LuaScript s_clearWithoutCommitCheckScript = LuaScript.Prepare(CLEAR_WITHOUT_COMMIT_BODY);

        public override async Task ClearOverrideAsync(string appName, string settingName, TDataCenter dataCenter, string commitId = null)
        {
            var key = GetOverrideKey(settingName, Tier, dataCenter);
            var db = GetRedisDb();
            var hashName = (RedisKey)GetRedisHashName(appName);

            try
            {
                if (commitId != null)
                {
                    await db.ScriptEvaluateAsync(s_clearWithCommitCheckScript, new
                    {
                        hashName,
                        commitKey = COMMIT_KEY,
                        commitId,
                        key,
                        newCommit = NewCommit()
                    });
                }
                else
                {
                    await db.ScriptEvaluateAsync(s_clearWithoutCommitCheckScript, new
                    {
                        hashName,
                        commitKey = COMMIT_KEY,
                        key,
                        newCommit = NewCommit()
                    });
                }
            }
            catch (RedisException ex)
            {
                throw new NFigException("Unable to clear override. Redis operation failed. " + appName + "." + settingName, ex);
            }

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

        internal string GetRedisHashName(string appName)
        {
            return RedisKeyPrefix + appName;
        }
    }
}
