# NFig.Redis

[![NuGet version](https://badge.fury.io/nu/NFig.Redis.svg)](http://badge.fury.io/nu/NFig.Redis)
[![Build status](https://ci.appveyor.com/api/projects/status/9erli0y6pmmig9wh/branch/master?svg=true)](https://ci.appveyor.com/project/bretcope/nfig-redis/branch/master)

NFig.Redis is a configuration/settings library which uses Redis to store configuration overrides and receive live-update notifications. It is built on top of [NFig](https://github.com/NFig/NFig).

For documentation on how to use NFig.Redis, see the [Sample Web Application](https://github.com/NFig/SampleWebApplication).

## How does it interact with Redis internally?

Only overrides are stored in Redis (defaults are always in code).

There is one hash per application name (application name is the first argument to `GetApplicationSettings()` and similar methods). The keys of the hash are are the setting names prefixed by the data center and tier which the override is applicable for.

Additionally, there is a `$commit` key inside each hash which changes anytime a setting override is set/updated or cleared, which makes it easy to know whether or not your application has the most up-to-date settings.

### Pub/Sub

Anytime an override changes, the app which made the change broadcasts on the `NFig-AppUpdate` pub/sub channel. The value of that broadcast is the application name. NFig.Redis subscribes to this when you call `SubscribeToAppSettings()`.

Since it's possible for a pub/sub to fail, it is best to also poll for changes on a regular interval. Eventually this will be built into NFig.Redis's subscribe functionality, but for simplicity sake (there are some edge cases/race conditions to handle) it was left out of the initial version and must be performed in application code.
