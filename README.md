# NFig.Redis

[![NuGet version](https://badge.fury.io/nu/NFig.Redis.svg)](http://badge.fury.io/nu/NFig.Redis)
[![Build status](https://ci.appveyor.com/api/projects/status/9erli0y6pmmig9wh/branch/master?svg=true)](https://ci.appveyor.com/project/bretcope/nfig-redis/branch/master)

NFig.Redis is a configuration/settings library which uses Redis to store configuration overrides and receive live-update notifications. It is built on top of [NFig](https://github.com/NFig/NFig).

## Basic Usage

> This assumes you already understand how to create a [settings class](https://github.com/NFig/SampleWebApplication#settings-class) and enums for [tier and data center](https://github.com/NFig/SampleWebApplication#tier-and-data-center). Follow those links first if you're unfamiliar.

Simply create a new instance of `NFigRedisStore` and subscribe to updates on the application, tier, and data center you care about.

```csharp
var nfig = new NFigRedisStore<Settings, Tier, DataCenter>("localhost:6379");
nfig.SubscribeToAppSettings("AppName", tier, dc, OnSettingsUpdate);
```

We also need to create the callback method:

```csharp
void OnSettingsUpdate(Exception ex, TSettings settings, NFigRedisStore<TSettings, TTier, TDataCenter> nfigRedisStore)
{
	if (ex != null)
	{
		// couldn't load the settings
		LogException(ex);
		return;
	}
	
	CurrentSettings = settings;
}
```

That's all you need for a simple case, but maybe we want to expand our error handling a little bit to make our app more resilient against potentially bad override values:

```csharp
void OnSettingsUpdate(Exception ex, TSettings settings, NFigRedisStore<TSettings, TTier, TDataCenter> nfigRedisStore)
{
	if (ex != null)
	{
		// check if we have invalid overrides
		var invalid = ex as InvalidSettingOverridesException<Tier, DataCenter>;
		if (invalid != null)
		{
			LogException(invalid);
			
			// let's automatically clear the bad overrides (you may or may not want to do this automatically)
			foreach (var o in invalid.Exceptions)
			{
				nfigRedisStore.ClearOverride(settings.ApplicationName, o.SettingName, o.Tier, o.DataCenter);
			}
		}
		else
		{
			// couldn't load the settings
			LogException(ex);
			return;
		}
	}
	
	CurrentSettings = settings;
}
```

That's all you really need to get up and running.

## Changes in 2.0

There are several, but easy to work around, breaking changes in 2.0 you should be aware of if you were using NFig/NFig.Redis 1.0. Read this section carefully.

#### GetApplicationSettings Renamed

`NFigStore.GetApplicationSettings` and `NFigAsyncStore.GetApplicationSettingsAsync` have been renamed to `GetAppSettings` and `GetAppSettingsAsync` to match other NFig API methods which all use `App` rather than `Application`. This applies to `NFigRedisStore` which inherits from both.

#### Contingency Polling

Previously, it was recommended that you setup a polling interval to periodically check for settings updates in case the Redis pub/sub failed or was missed. Now, this functionality is built-in to NFig.Redis.

It's referred to as contingency polling, and it defaults to checking once every 60 seconds. A different interval can be specified via an optional constructor argument.

#### SubscribeToAppSettings Changes

Turns out, the `overrideExisting` argument _never_ actually did anything, so it's been removed.

More than one unique callback method is permitted per app/tier/data center combination. Duplicates will be ignored.

If a callback is added (not a duplicate), then the provided callback will be called synchronously (with the current settings) before `SubscribeToAppSettings` returns. This means it is no longer necessary (or advisable) to call `GetAppSettings` in addition to subscribing.

#### Redis Key Prefix

Previously, the application name was used as the key name for storing overrides in Redis. Now, a prefix is included in the name (default is "NFig:"). This means, if you have existing overrides, you either need to rename the existing hash in Redis to include the prefix, or you can set the prefix to an empty string using the optional `redisKeyPrefix` constructor argument.

#### Invalid Override Protection

Although overrides are always validated at the time they are set, there are still scenarios where it's possible to have an invalid override (such as if you changed the setting property type while an override was live) which could cause settings to not load. For many apps, settings not loading may mean the entire app is down, and if the app is down, it becomes difficult to edit overrides.

To avoid this catch-22, NFig.Redis will substitute in default values when it can't apply an override (via NFig's new `TryGetAppSettings` method). If that happens, the first argument to the subscription callback will be a `InvalidSettingOverridesException`, but the settings argument will still be a valid `TSettings` object.

It's your choice what to do with this information. Automatically clearing the bad values and/or alerting humans to the problem are good suggestions. More detail about the invalid overrides can be found via the `InvalidSettingOverridesException.Exceptions` property which is an `IList<InvalidSettingValueException<TTier, TDataCenter>>` (see [NFig/Exceptions.cs](https://github.com/NFig/NFig/blob/master/NFig/Exceptions.cs)). Each exception in the list represents data about an invalid override.

> The `settings` argument is only guaranteed to be usable if the first argument (`Exception ex`) is either null or of type `InvalidSettingOverridesException<TTier, TDataCenter>` where `TTier` and `TDataCenter` match the type arguments used when instantiating NFig/NFigRedisStore. If `ex` is not null and of another type, you must consider the `settings` argument to be unreliable, regardless of whether or not it is null.

#### UnsubscribeFromAppSettings Returns Count

Although I'm not sure why you'd even use this method, it now returns a count of how many callbacks were removed rather than simply a boolean  `removedAny`.

## How does it interact with Redis internally?

Only overrides are stored in Redis (defaults are always in code).

There is one hash per application name. The name of the hash is a prefix (defaults to "NFig:") plus the application name (first argument to `GetApplicationSettings()` and similar methods). The keys of the hash are are the setting names prefixed by the data center and tier which the override is applicable for.

Additionally, there is a `$commit` key inside each hash which changes anytime a setting override is set/updated or cleared, which makes it easy to know whether or not your application has the most up-to-date settings.

### Pub/Sub

Anytime an override changes, the app which made the change broadcasts on the `NFig-AppUpdate` pub/sub channel. The value of that broadcast is the application name. NFig.Redis subscribes to this when you call `SubscribeToAppSettings()`.

Since it's possible for a pub/sub to fail, NFig.Redis also polls for changes (called contingency polling) on a regular interval (defaults to every 60 seconds).
