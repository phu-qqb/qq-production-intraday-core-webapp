using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace TradingDaemon.Services;

public static class ProcessRunner
{
    public static Task<(string StdOut, string StdErr, int ExitCode)> RunAsync(
        string fileName,
        string arguments,
        Action<string>? onOutput = null,
        Action<string>? onError = null) =>
        RunAsync(CreateProcess(fileName, arguments), onOutput, onError);

    public static Task<(string StdOut, string StdErr, int ExitCode)> RunAsync(
        string fileName,
        IEnumerable<string> arguments,
        Action<string>? onOutput = null,
        Action<string>? onError = null) =>
        RunAsync(CreateProcess(fileName, arguments), onOutput, onError);

    private static Process CreateProcess(string fileName, string arguments)
    {
        return new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            }
        };
    }

    private static Process CreateProcess(string fileName, IEnumerable<string> arguments)
    {
        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = fileName,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            }
        };

        foreach (var argument in arguments)
        {
            if (!string.IsNullOrWhiteSpace(argument))
            {
                process.StartInfo.ArgumentList.Add(argument);
            }
        }

        return process;
    }

    private static async Task<(string StdOut, string StdErr, int ExitCode)> RunAsync(
        Process process,
        Action<string>? onOutput,
        Action<string>? onError)
    {
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
