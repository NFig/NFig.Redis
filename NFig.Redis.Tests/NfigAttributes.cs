


using NFig;

namespace NFig.Redis.Tests
{
    public class DataCenterAttribute : DefaultSettingValueAttribute
    {
        public DataCenterAttribute(DataCenter dataCenter, object defaultValue)
        {
            DataCenter = dataCenter;
            DefaultValue = defaultValue;
        }
    }

    public class TierAttribute : DefaultSettingValueAttribute
    {
        public TierAttribute(Tier tier, object defaultValue)
        {
            Tier = tier;
            DefaultValue = defaultValue;
        }
    }

    public class TierDataCenterAttribute : DefaultSettingValueAttribute
    {
        public TierDataCenterAttribute(Tier tier, DataCenter dataCenter, object defaultValue)
        {
            Tier = tier;
            DataCenter = dataCenter;
            DefaultValue = defaultValue;
        }
    }
}