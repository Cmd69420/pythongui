import tkinter as tk
from tkinter import messagebox
from tally_client import TallyClient
from parser import parse_ledgers
from geocoder import geocode_dataframe
import pandas as pd

OUTPUT_FILE = "tally_debtors_creditors.csv"


class ExtractWindow:
    def __init__(self, parent, company_name):
        self.company = company_name
        self.client = TallyClient()
        

        self.win = tk.Toplevel(parent)
        self.win.title("Extract Ledgers")
        self.win.geometry("400x300")

        tk.Label(self.win, text="Extract Ledgers", font=("Arial", 14, "bold")).pack(pady=10)

        tk.Label(self.win, text="Username").pack()
        self.user_entry = tk.Entry(self.win)
        self.user_entry.pack()

        tk.Label(self.win, text="Password").pack()
        self.pass_entry = tk.Entry(self.win, show="*")
        self.pass_entry.pack()
        self.do_geocode = tk.BooleanVar(value=True)

        tk.Checkbutton(
            self.win,
            text="Enable Geocoding (Lat / Lng)",
            variable=self.do_geocode
        ).pack()


        self.status = tk.Label(self.win, text="", fg="blue")
        self.status.pack(pady=10)

        tk.Button(self.win, text="Start Extraction", command=self.start).pack(pady=10)

    def start(self):
        user = self.user_entry.get().strip()
        pw = self.pass_entry.get().strip()

        if not user or not pw:
            messagebox.showerror("Error", "Username and password required")
            return

        try:
            self.status.config(text="Fetching ledgers from Tally...")
            self.win.update()

            xml = self.client.fetch_ledgers(self.company, user, pw)


            self.status.config(text="Parsing data...")
            self.win.update()

            df = parse_ledgers(xml)

            if self.do_geocode.get():
                self.status.config(text="Geocoding addresses...")
                self.win.update()

                df = geocode_dataframe(df)

            df.to_csv(OUTPUT_FILE, index=False)


            self.status.config(text="Saved successfully ✔")
            messagebox.showinfo("Success", f"Saved to {OUTPUT_FILE}")

        except Exception as e:
            messagebox.showerror("Error", str(e))
