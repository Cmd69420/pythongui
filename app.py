from tkinter import Tk, Label, Button, StringVar, ttk, messagebox
from tally_client import TallyClient
from server_client import server_health_check
from config import TALLY_URL, SERVER_HEALTH_URL
from extract_window import ExtractWindow

class MiddlewareApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Tally Middleware")
        self.root.geometry("500x320")

        self.tally = TallyClient(TALLY_URL)

        self.tally_status = StringVar(value="Checking...")
        self.server_status = StringVar(value="Checking...")
        self.company_var = StringVar()

        Label(root, text="Tally Middleware", font=("Arial", 16, "bold")).pack(pady=10)

        Label(root, textvariable=self.tally_status).pack()
        Label(root, textvariable=self.server_status).pack(pady=5)

        Button(root, text="Refresh Status", command=self.refresh_status).pack(pady=5)

        Label(root, text="Select Company").pack(pady=10)
        self.company_dropdown = ttk.Combobox(root, textvariable=self.company_var, width=45)
        self.company_dropdown.pack()

        Button(root, text="Next (Extract)", command=self.next_step).pack(pady=20)

        self.refresh_status()

    def refresh_status(self):
        if self.tally.test_connection():
            self.tally_status.set("Tally: Connected ✅")
            self.load_companies()
        else:
            self.tally_status.set("Tally: Not Connected ❌")

        if server_health_check(SERVER_HEALTH_URL):
            self.server_status.set("Server: Connected ✅")
        else:
            self.server_status.set("Server: Not Reachable ❌")

    def load_companies(self):
        try:
            companies = self.tally.get_companies()
            self.company_dropdown["values"] = companies
            if companies:
                self.company_dropdown.current(0)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load companies\n{e}")

    def next_step(self):
        company = self.company_var.get()
        ExtractWindow(root, company)
        if not company:
            messagebox.showwarning("Select Company", "Please select a company")
            return

        messagebox.showinfo(
            "Next Step",
            f"Company selected:\n{company}\n\nNext: Credentials + Extraction"
        )

if __name__ == "__main__":
    root = Tk()
    app = MiddlewareApp(root)
    root.mainloop()
