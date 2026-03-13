import os
import shutil
import subprocess
import sys
import webbrowser


def _run_command(command):
    try:
        subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except Exception:
        return False


def _macos_private_commands(url):
    commands = []
    app_flags = [
        ("Microsoft Edge", "--inprivate"),
        ("Google Chrome", "--incognito"),
        ("Brave Browser", "--incognito"),
        ("Firefox", "--private-window"),
    ]
    for app_name, flag in app_flags:
        commands.append(["open", "-na", app_name, "--args", flag, url])
    return commands


def _windows_private_commands(url):
    commands = []
    browser_flags = [
        ("msedge", "--inprivate"),
        ("chrome", "--incognito"),
        ("brave", "--incognito"),
        ("firefox", "--private-window"),
    ]
    for browser, flag in browser_flags:
        commands.append(["cmd", "/c", "start", "", browser, flag, url])
    return commands


def _linux_private_commands(url):
    commands = []
    browser_flags = [
        ("microsoft-edge", "--inprivate"),
        ("google-chrome", "--incognito"),
        ("chromium-browser", "--incognito"),
        ("chromium", "--incognito"),
        ("brave-browser", "--incognito"),
        ("firefox", "--private-window"),
    ]
    for browser, flag in browser_flags:
        if shutil.which(browser):
            commands.append([browser, flag, url])
    return commands


def open_url_in_private_window(url):
    platform = sys.platform

    if platform == "darwin":
        commands = _macos_private_commands(url)
    elif os.name == "nt":
        commands = _windows_private_commands(url)
    else:
        commands = _linux_private_commands(url)

    for command in commands:
        if _run_command(command):
            return "private"

    try:
        if webbrowser.open(url):
            return "default"
        return None
    except Exception:
        return None
