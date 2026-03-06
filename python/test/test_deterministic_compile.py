import os
import subprocess
import sys
import tempfile
import textwrap


SET_PRINT = 'columns = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}; print(", ".join(columns))'

# Mirrors the os.execv fix in zipline.py main(): re-execs with PYTHONHASHSEED=0 if not already set.
EXECV_SCRIPT = textwrap.dedent(f"""\
    import os, sys
    if os.environ.get("PYTHONHASHSEED") != "0":
        os.environ["PYTHONHASHSEED"] = "0"
        os.execv(sys.executable, [sys.executable] + sys.argv)
    {SET_PRINT}
""")


def _run(script, hashseed=None):
    env = os.environ.copy()
    if hashseed is not None:
        env["PYTHONHASHSEED"] = str(hashseed)
    else:
        env.pop("PYTHONHASHSEED", None)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script)
        f.flush()
        result = subprocess.run(
            [sys.executable, f.name],
            capture_output=True, text=True, env=env,
        )
    os.unlink(f.name)
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    return result.stdout.strip()


def test_set_iteration_varies_without_fixed_hashseed():
    """Different PYTHONHASHSEED values produce different set iteration orders,
    demonstrating why the fix is necessary."""
    outputs = {_run(SET_PRINT, hashseed=seed) for seed in range(50)}
    assert len(outputs) > 1


def test_execv_reexec_produces_deterministic_output():
    """The os.execv re-exec with PYTHONHASHSEED=0 makes set iteration deterministic,
    regardless of the initial hash seed."""
    outputs = {_run(EXECV_SCRIPT, hashseed=seed) for seed in range(50)}
    assert len(outputs) == 1
