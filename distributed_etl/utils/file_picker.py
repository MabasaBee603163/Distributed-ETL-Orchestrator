from __future__ import annotations

from pathlib import Path


def pick_csv_path(*, title: str = "Select a CSV file") -> Path:
    """Open a native file picker dialog (Windows/macOS/Linux via Tk).

    Tkinter is part of the standard library on CPython.
    """

    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as e:  # pragma: no cover
        raise RuntimeError("tkinter is required for interactive file picking on this Python build") from e

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        path = filedialog.askopenfilename(
            title=title,
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
    finally:
        root.destroy()

    if not path:
        raise RuntimeError("No file selected")

    return Path(path)
