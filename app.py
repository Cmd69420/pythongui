from tkinter import Tk, Label, Button, StringVar, ttk, messagebox, Entry, Checkbutton, BooleanVar, Frame, Listbox, Scrollbar, MULTIPLE, END
from tally_client import TallyClient
from server_client import server_health_check
from config import TALLY_URL, SERVER_HEALTH_URL
from parser import parse_ledgers
from geocoder import geocode_dataframe
import threading

OUTPUT_FILE = "tally_export.csv"

# Master types available in Tally
MASTER_TYPES = {
    "Ledger": [
        "All Ledgers",
        "Sundry Debtors",
        "Sundry Creditors",
        "Bank Accounts",
        "Cash-in-Hand",
        "Current Assets",
        "Current Liabilities",
        "Fixed Assets",
        "Investments",
        "Loans (Liability)",
        "Duties & Taxes",
        "Capital Account",
        "Sales Accounts",
        "Purchase Accounts",
        "Direct Incomes",
        "Indirect Incomes",
        "Direct Expenses",
        "Indirect Expenses"
    ],
    "Group": ["All Groups"],
    "Stock Item": ["All Stock Items"],
    "Stock Group": ["All Stock Groups"],
    "Stock Category": ["All Stock Categories"],
    "Unit": ["All Units"],
    "Godown": ["All Godowns"],
    "Cost Centre": ["All Cost Centres"],
    "Voucher Type": ["All Voucher Types"]
}

class MiddlewareApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Tally Middleware - Advanced Export")
        self.root.geometry("700x700")

        self.tally = TallyClient(TALLY_URL)

        # Status variables
        self.tally_status = StringVar(value="Checking...")
        self.server_status = StringVar(value="Checking...")
        self.company_var = StringVar()
        self.master_type_var = StringVar()
        self.do_geocode = BooleanVar(value=True)
        self.extraction_status = StringVar(value="")
        self.is_company_secured = False  # Track if current company needs credentials

        # Build UI
        self.build_ui()
        
        # Initial status check
        self.refresh_status()

    def build_ui(self):
        # Header
        Label(self.root, text="Tally Middleware - Advanced Export", font=("Arial", 14, "bold")).pack(pady=10)

        # Status Section
        status_frame = Frame(self.root)
        status_frame.pack(pady=5)
        
        Label(status_frame, textvariable=self.tally_status).pack()
        Label(status_frame, textvariable=self.server_status).pack(pady=5)
        Button(status_frame, text="Refresh Status", command=self.refresh_status).pack(pady=5)

        ttk.Separator(self.root, orient='horizontal').pack(fill='x', pady=10)

        # Company Selection
        Label(self.root, text="Select Company", font=("Arial", 11, "bold")).pack(pady=5)
        self.company_dropdown = ttk.Combobox(self.root, textvariable=self.company_var, width=60)
        self.company_dropdown.pack()
        self.company_dropdown.bind("<<ComboboxSelected>>", self.on_company_selected)

        ttk.Separator(self.root, orient='horizontal').pack(fill='x', pady=10)

        # Master Type Selection
        master_selection_frame = Frame(self.root)
        master_selection_frame.pack(pady=5, fill='both', expand=True)

        Label(master_selection_frame, text="Select Master Type", font=("Arial", 11, "bold")).pack(pady=5)
        
        master_type_frame = Frame(master_selection_frame)
        master_type_frame.pack()
        
        self.master_type_dropdown = ttk.Combobox(
            master_type_frame, 
            textvariable=self.master_type_var, 
            values=list(MASTER_TYPES.keys()),
            width=30,
            state="readonly"
        )
        self.master_type_dropdown.pack(side='left', padx=5)
        self.master_type_dropdown.bind("<<ComboboxSelected>>", self.on_master_type_change)
        
        # Load Groups button (for Ledger type)
        self.load_groups_btn = Button(
            master_type_frame,
            text="🔄 Load Groups from Tally",
            command=self.load_groups_from_tally,
            state='disabled'
        )
        self.load_groups_btn.pack(side='left', padx=5)

        # Sub-category selection (multiselect)
        Label(master_selection_frame, text="Select Categories to Export", font=("Arial", 10, "bold")).pack(pady=(10,5))
        
        listbox_frame = Frame(master_selection_frame)
        listbox_frame.pack(pady=5, fill='both', expand=True)

        scrollbar = Scrollbar(listbox_frame)
        scrollbar.pack(side='right', fill='y')

        self.category_listbox = Listbox(
            listbox_frame, 
            selectmode=MULTIPLE, 
            height=8,
            yscrollcommand=scrollbar.set
        )
        self.category_listbox.pack(side='left', fill='both', expand=True, padx=10)
        scrollbar.config(command=self.category_listbox.yview)

        # Select All / Deselect All buttons
        btn_frame = Frame(master_selection_frame)
        btn_frame.pack(pady=5)
        Button(btn_frame, text="Select All", command=self.select_all_categories).pack(side='left', padx=5)
        Button(btn_frame, text="Deselect All", command=self.deselect_all_categories).pack(side='left', padx=5)

        ttk.Separator(self.root, orient='horizontal').pack(fill='x', pady=10)

        # Credentials Section
        Label(self.root, text="Tally Credentials", font=("Arial", 11, "bold")).pack(pady=5)
        
        cred_frame = Frame(self.root)
        cred_frame.pack()
        
        Label(cred_frame, text="Username:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.user_entry = Entry(cred_frame, width=30)
        self.user_entry.grid(row=0, column=1, padx=5, pady=5)

        Label(cred_frame, text="Password:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.pass_entry = Entry(cred_frame, show="*", width=30)
        self.pass_entry.grid(row=1, column=1, padx=5, pady=5)

        # Geocoding Option (only for Ledgers)
        self.geocode_check = Checkbutton(
            self.root,
            text="Enable Geocoding (Lat / Lng) - For Ledgers only",
            variable=self.do_geocode,
            state='disabled'
        )
        self.geocode_check.pack(pady=10)

        ttk.Separator(self.root, orient='horizontal').pack(fill='x', pady=10)

        # Extraction Status
        self.status_label = Label(self.root, textvariable=self.extraction_status, fg="blue", font=("Arial", 10))
        self.status_label.pack(pady=5)

        # Extract Button
        self.extract_btn = Button(
            self.root, 
            text="Start Extraction", 
            command=self.start_extraction,
            bg="#4CAF50",
            fg="white",
            font=("Arial", 11, "bold"),
            padx=20,
            pady=10
        )
        self.extract_btn.pack(pady=10)

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
                # Check security for first company
                self.on_company_selected()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load companies\n{e}")

    def on_company_selected(self, event=None):
        """Check if selected company requires authentication"""
        company = self.company_var.get()
        if not company:
            return
        
        try:
            self.extraction_status.set("Checking company security...")
            self.root.update()
            
            self.is_company_secured = self.tally.check_company_security(company)
            
            if self.is_company_secured:
                self.extraction_status.set("🔒 This company requires username and password")
                # Highlight credential fields
                self.user_entry.config(bg="#fff9e6")
                self.pass_entry.config(bg="#fff9e6")
            else:
                self.extraction_status.set("🔓 This company is not password protected")
                # Normal background
                self.user_entry.config(bg="white")
                self.pass_entry.config(bg="white")
                
        except Exception as e:
            print(f"Error checking security: {e}")
            self.extraction_status.set("")

    def on_master_type_change(self, event=None):
        """Load sub-categories when master type is selected"""
        master_type = self.master_type_var.get()
        
        # Clear previous selections
        self.category_listbox.delete(0, END)
        
        # Load default categories
        if master_type in MASTER_TYPES:
            categories = MASTER_TYPES[master_type]
            for cat in categories:
                self.category_listbox.insert(END, cat)
        
        # Enable "Load Groups" button for Ledger type
        if master_type == "Ledger":
            self.load_groups_btn.config(state='normal')
            self.geocode_check.config(state='normal')
        else:
            self.load_groups_btn.config(state='disabled')
            self.geocode_check.config(state='disabled')
            self.do_geocode.set(False)

    def load_groups_from_tally(self):
        """Fetch actual groups from Tally when button is clicked"""
        company = self.company_var.get()
        
        if not company:
            messagebox.showwarning("Select Company", "Please select a company first")
            return
        
        # Check if company is secured and credentials are missing
        user = self.user_entry.get().strip()
        pw = self.pass_entry.get().strip()
        
        if self.is_company_secured and (not user or not pw):
            messagebox.showwarning(
                "Credentials Required",
                "This company is password protected.\nPlease enter username and password first."
            )
            return
        
        try:
            self.extraction_status.set("Loading groups from Tally...")
            self.load_groups_btn.config(state='disabled', text="Loading...")
            self.root.update()
            
            # Try to fetch groups
            groups = self.tally.fetch_groups(company, user, pw)
            
            # Clear and reload with actual groups
            self.category_listbox.delete(0, END)
            self.category_listbox.insert(END, "All Ledgers")
            for group in groups:
                self.category_listbox.insert(END, group)
            
            self.extraction_status.set(f"✅ Loaded {len(groups)} groups from Tally")
            messagebox.showinfo("Success", f"Loaded {len(groups)} groups from Tally!\n(Including any custom groups)")
            
        except Exception as e:
            error_msg = str(e)
            if "authentication" in error_msg.lower() or "login" in error_msg.lower() or "security" in error_msg.lower():
                messagebox.showerror(
                    "Authentication Required", 
                    "Invalid credentials or authentication failed.\nPlease check username/password and try again."
                )
            else:
                messagebox.showerror("Error", f"Failed to load groups:\n{error_msg}\n\nUsing default groups instead.")
            self.extraction_status.set("❌ Using default groups")
        finally:
            self.load_groups_btn.config(state='normal', text="🔄 Load Groups from Tally")

    def select_all_categories(self):
        self.category_listbox.select_set(0, END)

    def deselect_all_categories(self):
        self.category_listbox.select_clear(0, END)

    def get_selected_categories(self):
        """Get list of selected categories"""
        selected_indices = self.category_listbox.curselection()
        return [self.category_listbox.get(i) for i in selected_indices]

    def start_extraction(self):
        company = self.company_var.get()
        user = self.user_entry.get().strip()
        pw = self.pass_entry.get().strip()
        master_type = self.master_type_var.get()
        selected_categories = self.get_selected_categories()

        # Validation
        if not company:
            messagebox.showwarning("Select Company", "Please select a company")
            return
        
        if not master_type:
            messagebox.showwarning("Select Master Type", "Please select a master type to export")
            return
        
        if not selected_categories:
            messagebox.showwarning("Select Categories", "Please select at least one category to export")
            return
        
        # Check if secured company requires credentials
        if self.is_company_secured and (not user or not pw):
            messagebox.showerror(
                "Credentials Required",
                "This company is password protected.\nPlease enter username and password."
            )
            return

        # Disable button during extraction
        self.extract_btn.config(state='disabled')

        # Run extraction in separate thread
        thread = threading.Thread(
            target=self._run_extraction,
            args=(company, user, pw, master_type, selected_categories),
            daemon=True
        )
        thread.start()

    def _run_extraction(self, company, user, pw, master_type, categories):
        """Runs in background thread"""
        try:
            print(f"DEBUG: Starting extraction for {master_type}")
            print(f"DEBUG: Categories: {categories}")
            
            self._update_status(f"Fetching {master_type} data from Tally...")

            # Fetch data based on master type
            if master_type == "Ledger":
                print(f"DEBUG: Fetching all ledgers...")
                xml = self.tally.fetch_ledgers(company, user, pw)
                print(f"DEBUG: Received XML length: {len(xml)}")
                
                self._update_status("Parsing ledger data...")
                df = parse_ledgers(xml)
                print(f"DEBUG: Parsed {len(df)} ledgers")
                
                # Filter by selected categories
                self._update_status("Filtering selected categories...")
                if categories and "All Ledgers" not in categories:
                    # Filter to only selected parent groups (case-insensitive)
                    categories_lower = [c.lower() for c in categories]
                    df = df[df['parent'].str.lower().isin(categories_lower)]
                    print(f"DEBUG: Filtered to {len(df)} ledgers from categories: {categories}")
                
                # Geocode if enabled
                if self.do_geocode.get():
                    self._update_status("Geocoding addresses...")
                    df = geocode_dataframe(df)
                    print(f"DEBUG: Geocoding complete")
            else:
                # For other master types, use generic fetch
                print(f"DEBUG: Fetching {master_type} masters...")
                xml = self.tally.fetch_masters(company, user, pw, master_type)
                print(f"DEBUG: Received XML length: {len(xml)}")
                
                self._update_status(f"Parsing {master_type} data...")
                df = self._parse_generic_master(xml, master_type)
                print(f"DEBUG: Parsed {len(df)} records")

            # Save to CSV
            filename = f"tally_{master_type.lower().replace(' ', '_')}_{company[:20]}.csv"
            df.to_csv(filename, index=False)
            print(f"DEBUG: Saved to {filename}")

            self._update_status(f"✅ Saved successfully to {filename}")
            
            self.root.after(0, lambda: messagebox.showinfo(
                "Success", 
                f"Exported {len(df)} {master_type} records\n"
                f"Categories: {', '.join(categories)}\n"
                f"Saved to {filename}"
            ))

        except Exception as e:
            print(f"DEBUG ERROR: {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
            
            self._update_status("❌ Extraction failed")
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.root.after(0, lambda: messagebox.showerror("Error", error_msg))
        
        finally:
            self.root.after(0, lambda: self.extract_btn.config(state='normal'))

    def _parse_generic_master(self, xml, master_type):
        """Placeholder for parsing other master types"""
        import pandas as pd
        from lxml import etree
        
        parser = etree.XMLParser(recover=True, huge_tree=True)
        root = etree.fromstring(xml.encode(), parser)
        
        # Basic parsing - you can enhance this
        rows = []
        for elem in root.findall(f".//{master_type.upper().replace(' ', '')}"):
            rows.append({
                "name": elem.get("NAME", ""),
                "guid": elem.findtext("GUID", "")
            })
        
        return pd.DataFrame(rows)

    def _update_status(self, message):
        """Thread-safe status update"""
        self.root.after(0, lambda: self.extraction_status.set(message))


if __name__ == "__main__":
    root = Tk()
    app = MiddlewareApp(root)
    root.mainloop()