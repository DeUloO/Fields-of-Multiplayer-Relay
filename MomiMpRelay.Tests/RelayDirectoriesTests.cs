using MomiMpRelay.Configuration;

namespace MomiMpRelay.Tests;

public sealed class RelayDirectoriesTests
{
    [Theory]
    [InlineData("main")]
    [InlineData("")]
    [InlineData("!!!")]
    public void MainOrEmptyInstanceUsesDefaultDirectory(string instanceId)
    {
        var path = RelayDirectories.InstanceDir(instanceId);

        Assert.EndsWith(Path.Combine("FieldsOfMistria", "momi_mp"), path);
    }

    [Fact]
    public void InstanceIdIsSanitized()
    {
        var path = RelayDirectories.InstanceDir("beta/one with spaces");

        Assert.EndsWith(Path.Combine("FieldsOfMistria", "momi_mp_betaonewithspaces"), path);
    }
}
