"""Options shared by :func:`openqvd.read`, :func:`openqvd.schema` and
:func:`openqvd.write`."""

from __future__ import annotations

from typing import FrozenSet, Iterable, Optional, Union

from openqvd._openqvd import ReadWarning  # noqa: F401
from openqvd._openqvd import lenient_checks as _lenient_checks

__all__ = ["Leniency", "LeniencyLike", "ReadWarning"]


class Leniency:
    """The set of checks a caller accepts being skipped.

    The default everywhere is strict: ``Leniency.strict()``, the empty set.
    A lenient read never skips a check silently: each tolerated deviation is
    reported as a :class:`ReadWarning` and as a :class:`openqvd.QvdWarning`.

    ``Leniency.lenient()`` allows every check this version knows about. Names
    of individual checks are in :data:`Leniency.CHECKS`; new ones may be added
    in minor releases, so name the checks you rely on if you need a fixed
    behavior.
    """

    CHECKS: FrozenSet[str] = frozenset(_lenient_checks())

    __slots__ = ("_checks",)

    def __init__(self, checks: Iterable[str] = ()) -> None:
        checks = frozenset(checks)
        unknown = checks - self.CHECKS
        if unknown:
            raise ValueError(
                f"unknown lenient check(s) {sorted(unknown)!r}; "
                f"known checks: {sorted(self.CHECKS)!r}"
            )
        self._checks = checks

    @classmethod
    def strict(cls) -> "Leniency":
        """No check may be skipped."""
        return cls()

    @classmethod
    def lenient(cls) -> "Leniency":
        """Every check known to this version may be skipped."""
        return cls(cls.CHECKS)

    @property
    def checks(self) -> FrozenSet[str]:
        """Names of the allowed checks."""
        return self._checks

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Leniency):
            return self._checks == other._checks
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._checks)

    def __bool__(self) -> bool:
        """True when at least one check may be skipped."""
        return bool(self._checks)

    def __repr__(self) -> str:
        if not self._checks:
            return "Leniency.strict()"
        return f"Leniency({sorted(self._checks)!r})"


LeniencyLike = Union[Leniency, Iterable[str], None]


def _check_names(leniency: LeniencyLike) -> Optional[list]:
    """Normalise a ``leniency=`` argument to a list of check names."""
    if leniency is None:
        return None
    if isinstance(leniency, str):
        raise TypeError(
            "leniency must be a Leniency or an iterable of check names, "
            "not a single string"
        )
    if not isinstance(leniency, Leniency):
        leniency = Leniency(leniency)
    return sorted(leniency.checks)
