#!/usr/bin/env python3
"""
Python Environment Diagnostic Tool (Pytest Compatible)

Run this with:
    pytest environment_test.py -v
or
    python -m pytest environment_test.py -v
"""

import os
from pathlib import Path
import platform
import site
import subprocess
import sys

import pytest


def horizontal_line(char="-", length=80):
    """Print a horizontal line for better readability."""
    return char * length


def section_header(title):
    """Format a section header."""
    return f"\n{horizontal_line()}\n### {title} ###\n{horizontal_line()}"


def get_package_versions():
    """Get versions of key packages that might affect testing."""
    packages = ["pytest", "pytest-cov", "pytest-mock", "pytest-xdist"]
    results = {}

    for package in packages:
        try:
            module = __import__(package)
            results[package] = getattr(module, "__version__", "Unknown")
        except ImportError:
            results[package] = "Not installed"

    return results


def get_installed_packages():
    """Get a list of all installed packages using pip."""
    try:
        result = subprocess.run([sys.executable, "-m", "pip", "list"], capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.SubprocessError:
        return "Failed to retrieve installed packages"


def generate_diagnostic_report():
    """Generate comprehensive information about the Python environment."""
    report = []

    # Header
    report.append("\n\nPYTHON ENVIRONMENT DIAGNOSTIC REPORT")
    report.append(f"Generated on: {platform.node()} at {os.path.basename(sys.executable)}")
    report.append(horizontal_line("=", 80))

    # Basic Python information
    report.append(section_header("PYTHON INTERPRETER INFORMATION"))
    report.append(f"Python Executable:      {sys.executable}")
    report.append(f"Python Version:         {sys.version}")
    report.append(f"Python Implementation:  {platform.python_implementation()}")
    report.append(f"Platform:               {platform.platform()}")

    # Working directory information
    report.append(section_header("WORKING DIRECTORY"))
    report.append(f"Current Working Dir:    {os.getcwd()}")
    report.append(f"Script Location:        {os.path.abspath(__file__)}")

    # Module search paths
    report.append(section_header("PYTHON PATH"))
    for i, path in enumerate(sys.path, 1):
        report.append(f"{i:2d}. {path}")

    # Site packages
    report.append(section_header("SITE PACKAGES"))
    for path in site.getsitepackages():
        report.append(path)

    # Environment variables
    report.append(section_header("ENVIRONMENT VARIABLES"))
    env_vars = {
        k: v
        for k, v in os.environ.items()
        if k.startswith(("PY", "PYTEST", "PATH", "PYTHONPATH", "VIRTUAL_ENV", "CONDA"))
        or k in ["LANG", "LC_ALL", "SHELL", "TERM", "USER", "HOME"]
    }
    for key, value in sorted(env_vars.items()):
        report.append(f"{key}: {value}")

    # VSCode specific info
    report.append(section_header("VSCODE INFORMATION"))
    vscode_process = os.environ.get("VSCODE_PID", "Not running in VSCode")
    report.append(f"VSCode Process ID:      {vscode_process}")
    report.append(f"VSCode Debug:           {'DEBUGPY' in sys.modules}")
    report.append(f"VSCode Extensions:      {os.environ.get('VSCODE_EXTENSIONS', 'Not available')}")

    # Virtual environment
    report.append(section_header("VIRTUAL ENVIRONMENT"))
    venv = os.environ.get("VIRTUAL_ENV", "Not in a virtual environment")
    report.append(f"Virtual Env:            {venv}")

    # Pytest information
    report.append(section_header("PYTEST INFORMATION"))
    pytest_packages = get_package_versions()
    for package, version in pytest_packages.items():
        report.append(f"{package}: {version}")

    # Pytest plugins
    report.append(section_header("PYTEST PLUGINS"))
    try:
        plugin_list = []
        for plugin_name, plugin_obj in pytest.config.pluginmanager.name2plugin.items():
            plugin_list.append(f"{plugin_name}: {getattr(type(plugin_obj), '__module__', 'unknown')}")
        for plugin in sorted(plugin_list):
            report.append(plugin)
    except:
        report.append("Could not access pytest plugin information")

    # Look for pytest configuration files
    report.append(section_header("PYTEST CONFIGURATION FILES"))
    config_files = ["pytest.ini", "pyproject.toml", "tox.ini", "conftest.py", ".coveragerc"]

    cwd = Path(os.getcwd())
    for file in config_files:
        file_path = cwd / file
        if file_path.exists():
            report.append(f"{file}: Found at {file_path}")
            if file == "pyproject.toml":
                try:
                    with open(file_path, "r") as f:
                        content = f.read()
                        if "pytest" in content:
                            report.append("  - Contains pytest configuration")
                except Exception as e:
                    report.append(f"  - Error reading file: {e}")
        else:
            report.append(f"{file}: Not found")

    # All installed packages
    report.append(section_header("INSTALLED PACKAGES"))
    report.append(get_installed_packages())

    report.append(horizontal_line("=", 80))
    report.append("\nSave this output to compare between VSCode and terminal environments.\n")

    return "\n".join(report)


def test_generate_environment_report(capsys):
    """Pytest test that generates and displays the environment report."""
    # Print report to stdout so it appears in pytest output
    print(generate_diagnostic_report())

    # This assertion always passes - the test is just a vehicle to run the report
    assert True, "This test is a diagnostic tool - it should always pass"


if __name__ == "__main__":
    # If run directly (not through pytest), print the report
    print(generate_diagnostic_report())
