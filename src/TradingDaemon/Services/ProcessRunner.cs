using System.Diagnostics;

namespace TradingDaemon.Services;

public static class ProcessRunner
{
    public static async Task<(string StdOut, string StdErr, int ExitCode)> RunAsync(string fileName, string arguments)
    {
        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            }
        };

        process.Start();
        var output = await process.StandardOutput.ReadToEndAsync();
        var error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        return (output, error, process.ExitCode);
    }
}
