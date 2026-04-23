# Claude Code Configuration

This directory contains Claude Code configuration for the PySpark demo repository.

## Features Configured

### 1. Automatic Virtual Environment Check
A pre-execution hook checks if the virtual environment is activated before running Python commands. If not activated, you'll get a warning and suggestion to run:
```bash
source venv/bin/activate
```

### 2. Pre-Approved Permissions
The following commands are pre-approved and won't prompt for permission:
- `python`, `python3` - Running Python scripts
- `pip`, `pip3` - Installing packages
- `pytest` - Running tests
- `source venv/bin/activate` - Activating virtual environment
- `./run_example.sh` - Helper script
- Reading/editing Python files in `src/`

### 3. Custom Slash Commands

#### `/run-pyspark <script-name>`
Run a PySpark example from `src/pyspark_examples/`
```bash
/run-pyspark SparkJoins.py
```

#### `/run-python <script-name>`
Run a Python example from `src/python_examples/`
```bash
/run-python decorator_exmpl.py
```

#### `/run-sql <script-name>`
Run a SQL example from `src/SQL_Examples/`
```bash
/run-sql SQL_EmpDeptMgr.py
```

#### `/run-threading <script-name>`
Run a threading example from `src/python_Threading/`
```bash
/run-threading 01_threading.py
```

### 4. Environment Variables
- `PYTHONPATH` is automatically set to include `${workspaceFolder}/src`

## Files

- `settings.json` - Main project configuration (committed to git)
- `settings.local.json` - Personal overrides (gitignored, create if needed)
- `skills/` - Custom slash commands for running examples

## Managing Settings

To add personal overrides without affecting the team:
```bash
# Create settings.local.json (gitignored)
touch .claude/settings.local.json
```

Example personal override:
```json
{
  "permissions": {
    "defaultMode": "auto"
  }
}
```

## Useful Commands

View all configured hooks:
```bash
jq '.hooks' .claude/settings.json
```

View configured permissions:
```bash
jq '.permissions' .claude/settings.json
```

List available custom commands:
```bash
ls -1 .claude/skills/
```
