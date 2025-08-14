using System.Diagnostics;
using System.Text;

namespace TradingDaemon.Services;

public static class ProcessRunner
{
    public static async Task<(string StdOut, string StdErr, int ExitCode)> RunAsync(
        string fileName,
        string arguments,
        Action<string>? onOutput = null,
        Action<string>? onError = null)
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

        var stdOut = new StringBuilder();
        var stdErr = new StringBuilder();
        var outTcs = new TaskCompletionSource<bool>();
        var errTcs = new TaskCompletionSource<bool>();

        process.OutputDataReceived += (sender, e) =>
        {
            if (e.Data == null)
            {
                outTcs.TrySetResult(true);
            }
            else
            {
                stdOut.AppendLine(e.Data);
                onOutput?.Invoke(e.Data);
            }
        };

        process.ErrorDataReceived += (sender, e) =>
        {
            if (e.Data == null)
            {
                errTcs.TrySetResult(true);
            }
            else
            {
                stdErr.AppendLine(e.Data);
                onError?.Invoke(e.Data);
            }
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        await Task.WhenAll(process.WaitForExitAsync(), outTcs.Task, errTcs.Task);

        return (stdOut.ToString(), stdErr.ToString(), process.ExitCode);
    }
}
