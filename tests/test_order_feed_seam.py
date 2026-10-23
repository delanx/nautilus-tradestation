"""
Tests for ``_report_order_feed_state``: the optional host hook named by
``TRADESTATION_ORDER_FEED_HOOK`` ("pkg.module:function").

This pins the hook's outcomes:
  1. No hook configured (the default, and this repo's own test suite):
     silent, no import, no warning.
  2. Hook configured but its module is not importable in this process:
     silent, no warning (same as having no host).
  3. Hook module importable but the function is missing, or the call raises
     (the most likely real-world cause: version skew between this adapter
     and the host): a ONE-TIME stdout warning naming the account and the
     likely cause, never a crash, never repeated.
  4. A working hook: the call reaches it with the exact arguments, no
     warning.
"""
import sys
import types

import pytest

from nautilus_tradestation import execution as execution_module

_HOOK_MODULE = "fake_host_app.order_feed"
_HOOK_TARGET = _HOOK_MODULE + ":set_order_feed_state"


@pytest.fixture(autouse=True)
def _reset_seam_warned_flag():
    """``_SEAM_WARNED`` is a module-global, warn-ONCE-per-process latch --
    reset it around every test so one test's warning does not silence the
    next test's assertion that a warning fires."""
    execution_module._SEAM_WARNED = False
    yield
    execution_module._SEAM_WARNED = False


def _install_fake_host_module(monkeypatch, module_obj, target=_HOOK_TARGET):
    """Install a fake host module into sys.modules and point the hook env var
    at it for the duration of one test."""
    pkg = types.ModuleType("fake_host_app")
    pkg.order_feed = module_obj
    monkeypatch.setitem(sys.modules, "fake_host_app", pkg)
    monkeypatch.setitem(sys.modules, _HOOK_MODULE, module_obj)
    monkeypatch.setenv(execution_module.ORDER_FEED_HOOK_ENV, target)


class TestNoHookIsSilent:
    def test_env_name_is_the_documented_one(self):
        assert execution_module.ORDER_FEED_HOOK_ENV == "TRADESTATION_ORDER_FEED_HOOK"

    def test_unset_env_is_silent_and_imports_nothing(self, monkeypatch, capsys):
        monkeypatch.delenv(execution_module.ORDER_FEED_HOOK_ENV, raising=False)
        monkeypatch.delitem(sys.modules, _HOOK_MODULE, raising=False)
        execution_module._report_order_feed_state(
            "SIM_FUTURES", proxy="jsonl:/feed/SIM_FUTURES", degraded=False
        )
        assert _HOOK_MODULE not in sys.modules
        assert "ORDER-FEED-WARNING" not in capsys.readouterr().out

    def test_unimportable_hook_module_is_silent(self, monkeypatch, capsys):
        monkeypatch.setenv(
            execution_module.ORDER_FEED_HOOK_ENV, "no_such_host_module_xyz:set_state"
        )
        execution_module._report_order_feed_state(
            "SIM_FUTURES", proxy="jsonl:/feed/SIM_FUTURES", degraded=True
        )
        assert "ORDER-FEED-WARNING" not in capsys.readouterr().out


class TestBrokenHookIsWarnedLoudlyOnce:
    def test_missing_function_warns_once_with_a_diagnosis(self, monkeypatch, capsys):
        broken = types.SimpleNamespace()  # deliberately no set_order_feed_state
        _install_fake_host_module(monkeypatch, broken)

        execution_module._report_order_feed_state(
            "SIM_FUTURES", proxy="jsonl:/feed/SIM_FUTURES", degraded=True
        )
        out = capsys.readouterr().out
        assert "[ORDER-FEED-WARNING]" in out
        assert "SIM_FUTURES" in out
        assert "version skew" in out

    def test_target_without_function_name_warns(self, monkeypatch, capsys):
        _install_fake_host_module(monkeypatch, types.SimpleNamespace(), target=_HOOK_MODULE)
        execution_module._report_order_feed_state("SIM_FUTURES", proxy=None, degraded=True)
        assert "[ORDER-FEED-WARNING]" in capsys.readouterr().out

    def test_raising_hook_warns_and_never_raises(self, monkeypatch, capsys):
        def set_order_feed_state(account_id, *, proxy, degraded):
            raise RuntimeError("heartbeat store unavailable")

        _install_fake_host_module(
            monkeypatch, types.SimpleNamespace(set_order_feed_state=set_order_feed_state)
        )
        execution_module._report_order_feed_state("SIM_FUTURES", proxy=None, degraded=True)
        assert "[ORDER-FEED-WARNING]" in capsys.readouterr().out

    def test_second_failure_does_not_repeat_the_warning(self, monkeypatch, capsys):
        # Warned ONCE per process: this runs on every connect/degrade/
        # recover event, so a broken hook must not spam the log.
        _install_fake_host_module(monkeypatch, types.SimpleNamespace())

        execution_module._report_order_feed_state("SIM_FUTURES", proxy=None, degraded=True)
        execution_module._report_order_feed_state("SIM_FUTURES", proxy=None, degraded=False)
        execution_module._report_order_feed_state("SIM_FUTURES", proxy=None, degraded=True)
        out = capsys.readouterr().out
        assert out.count("[ORDER-FEED-WARNING]") == 1

    def test_never_raises_into_the_caller(self, monkeypatch):
        # Fail-open is the contract -- a broken hook must never take down
        # _connect()/_stream_order_fills.
        _install_fake_host_module(monkeypatch, types.SimpleNamespace())
        execution_module._report_order_feed_state(
            "SIM_FUTURES", proxy=None, degraded=True
        )  # must not raise


class TestWorkingHookIsSilentAndCorrect:
    def test_working_hook_receives_the_exact_arguments(self, monkeypatch, capsys):
        calls = []

        def set_order_feed_state(account_id, *, proxy, degraded):
            calls.append({"account_id": account_id, "proxy": proxy, "degraded": degraded})

        _install_fake_host_module(
            monkeypatch, types.SimpleNamespace(set_order_feed_state=set_order_feed_state)
        )

        execution_module._report_order_feed_state(
            "SIM_FUTURES", proxy="jsonl:/feed/SIM_FUTURES", degraded=True
        )
        assert calls == [
            {"account_id": "SIM_FUTURES", "proxy": "jsonl:/feed/SIM_FUTURES", "degraded": True}
        ]
        assert "ORDER-FEED-WARNING" not in capsys.readouterr().out
