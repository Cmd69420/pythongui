from tkinter import Tk, Label, Button, StringVar, ttk, messagebox, Entry, Checkbutton, BooleanVar, Frame, Listbox, Scrollbar, MULTIPLE, END, VERTICAL, BOTH, LEFT, RIGHT, TOP, BOTTOM, X, Y, Canvas, Radiobutton, IntVar
from tally_client import TallyClient
from server_client import server_health_check
from config import TALLY_URL, SERVER_HEALTH_URL
from parser import parse_ledgers
from geocoder import geocode_dataframe  # Now uses enhanced geocoder with Google Places
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
        self.root.geometry("750x700")  # Smaller window, but scrollable
        
        # Make window resizable
        self.root.minsize(700, 600)

        self.tally = TallyClient(TALLY_URL)

        # Status variables
        self.tally_status = StringVar(value="Checking...")
        self.server_status = StringVar(value="Checking...")
        self.company_var = StringVar()
        self.master_type_var = StringVar()
        self.do_geocode = BooleanVar(value=True)
        self.geocode_method = IntVar(value=1)  # 1=Enhanced, 2=Basic, 3=None
        self.extraction_status = StringVar(value="")
        self.is_company_secured = False  # Track if current company needs credentials

        # Build UI with scrollbar
        self.build_scrollable_ui()
        
        # Initial status check
        self.refresh_status()

    def build_scrollable_ui(self):
        # Create main container with scrollbar
        main_container = Frame(self.root)
        main_container.pack(fill=BOTH, expand=True)
        
        # Create canvas
        self.canvas = Canvas(main_container, highlightthickness=0)
        self.canvas.pack(side=LEFT, fill=BOTH, expand=True)
        
        # Add scrollbar
        scrollbar = Scrollbar(main_container, orient=VERTICAL, command=self.canvas.yview)
        scrollbar.pack(side=RIGHT, fill=Y)
        
        # Configure canvas
        self.canvas.configure(yscrollcommand=scrollbar.set)
        
        # Create frame inside canvas
        self.scrollable_frame = Frame(self.canvas)
        self.canvas_frame = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        
        # Configure scroll region when frame size changes
        self.scrollable_frame.bind("<Configure>", self.on_frame_configure)
        
        # Bind mousewheel to scroll
        self.canvas.bind_all("<MouseWheel>", self.on_mousewheel)
        
        # Bind canvas resize to update frame width
        self.canvas.bind("<Configure>", self.on_canvas_configure)
        
        # Build UI inside scrollable frame
        self.build_ui(self.scrollable_frame)
    
    def on_frame_configure(self, event=None):
        """Update scroll region when frame size changes"""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
    
    def on_canvas_configure(self, event):
        """Update frame width when canvas is resized"""
        canvas_width = event.width
        self.canvas.itemconfig(self.canvas_frame, width=canvas_width)
    
    def on_mousewheel(self, event):
        """Enable mousewheel scrolling"""
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def build_ui(self, parent):
        # Header
        Label(parent, text="Tally Middleware - Advanced Export", 
              font=("Arial", 14, "bold")).pack(pady=10)

        # Status Section
        status_frame = Frame(parent)
        status_frame.pack(pady=5)
        
        Label(status_frame, textvariable=self.tally_status).pack()
        Label(status_frame, textvariable=self.server_status).pack(pady=3)
        Button(status_frame, text="Refresh Status", 
               command=self.refresh_status, padx=10, pady=3).pack(pady=5)

        ttk.Separator(parent, orient='horizontal').pack(fill=X, pady=10, padx=20)

        # Company Selection
        Label(parent, text="Select Company", 
              font=("Arial", 11, "bold")).pack(pady=5)
        self.company_dropdown = ttk.Combobox(parent, textvariable=self.company_var, width=70)
        self.company_dropdown.pack(pady=5)
        self.company_dropdown.bind("<<ComboboxSelected>>", self.on_company_selected)

        ttk.Separator(parent, orient='horizontal').pack(fill=X, pady=10, padx=20)

        # Master Type Selection
        Label(parent, text="Select Master Type", 
              font=("Arial", 11, "bold")).pack(pady=5)
        
        master_type_frame = Frame(parent)
        master_type_frame.pack(pady=5)
        
        self.master_type_dropdown = ttk.Combobox(
            master_type_frame, 
            textvariable=self.master_type_var, 
            values=list(MASTER_TYPES.keys()),
            width=30,
            state="readonly"
        )
        self.master_type_dropdown.pack(side=LEFT, padx=5)
        self.master_type_dropdown.bind("<<ComboboxSelected>>", self.on_master_type_change)
        
        # Load Groups button (for Ledger type)
        self.load_groups_btn = Button(
            master_type_frame,
            text="🔄 Load Groups",
            command=self.load_groups_from_tally,
            state='disabled',
            padx=10,
            pady=5
        )
        self.load_groups_btn.pack(side=LEFT, padx=5)

        # Sub-category selection (multiselect)
        Label(parent, text="Select Categories to Export", 
              font=("Arial", 10, "bold")).pack(pady=10)
        
        listbox_frame = Frame(parent)
        listbox_frame.pack(pady=5, padx=20, fill=X)

        list_scrollbar = Scrollbar(listbox_frame)
        list_scrollbar.pack(side=RIGHT, fill=Y)

        self.category_listbox = Listbox(
            listbox_frame, 
            selectmode=MULTIPLE, 
            height=8,
            yscrollcommand=list_scrollbar.set,
            font=("Arial", 9)
        )
        self.category_listbox.pack(side=LEFT, fill=BOTH, expand=True)
        list_scrollbar.config(command=self.category_listbox.yview)

        # Select All / Deselect All buttons
        btn_frame = Frame(parent)
        btn_frame.pack(pady=8)
        Button(btn_frame, text="Select All", 
               command=self.select_all_categories, padx=15, pady=3).pack(side=LEFT, padx=5)
        Button(btn_frame, text="Deselect All", 
               command=self.deselect_all_categories, padx=15, pady=3).pack(side=LEFT, padx=5)

        ttk.Separator(parent, orient='horizontal').pack(fill=X, pady=10, padx=20)

        # Credentials Section
        Label(parent, text="Tally Credentials", 
              font=("Arial", 11, "bold")).pack(pady=5)
        
        cred_frame = Frame(parent)
        cred_frame.pack(pady=5)
        
        Label(cred_frame, text="Username:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.user_entry = Entry(cred_frame, width=35)
        self.user_entry.grid(row=0, column=1, padx=5, pady=5)

        Label(cred_frame, text="Password:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.pass_entry = Entry(cred_frame, show="*", width=35)
        self.pass_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Separator(parent, orient='horizontal').pack(fill=X, pady=10, padx=20)

        # Geocoding Options (only for Ledgers) - WITH METHODS
        geocode_frame = Frame(parent, relief="solid", borderwidth=2, bg="#f0f8ff")
        geocode_frame.pack(pady=10, padx=30, fill=X)
        
        Label(
            geocode_frame,
            text="🌍 Geocoding Options (For Comparison)",
            font=("Arial", 11, "bold"),
            bg="#f0f8ff"
        ).pack(pady=8)
        
        # Radio buttons for methods
        radio_frame = Frame(geocode_frame, bg="#f0f8ff")
        radio_frame.pack(pady=5)
        
        self.geocode_radio1 = Radiobutton(
            radio_frame,
            text="🚀 Enhanced (Google Places + Address Geocoding)",
            variable=self.geocode_method,
            value=1,
            font=("Arial", 9),
            bg="#f0f8ff",
            state='disabled'
        )
        self.geocode_radio1.pack(anchor='w', padx=20, pady=2)
        
        Label(
            radio_frame,
            text="   → Searches business on Google first, then falls back to address",
            font=("Arial", 8),
            fg="#555",
            bg="#f0f8ff"
        ).pack(anchor='w', padx=40)
        
        self.geocode_radio2 = Radiobutton(
            radio_frame,
            text="📍 Basic (Address Geocoding Only)",
            variable=self.geocode_method,
            value=2,
            font=("Arial", 9),
            bg="#f0f8ff",
            state='disabled'
        )
        self.geocode_radio2.pack(anchor='w', padx=20, pady=2)
        
        Label(
            radio_frame,
            text="   → Only geocodes the address from Tally (original method)",
            font=("Arial", 8),
            fg="#555",
            bg="#f0f8ff"
        ).pack(anchor='w', padx=40)
        
        self.geocode_radio3 = Radiobutton(
            radio_frame,
            text="❌ No Geocoding",
            variable=self.geocode_method,
            value=3,
            font=("Arial", 9),
            bg="#f0f8ff",
            state='disabled'
        )
        self.geocode_radio3.pack(anchor='w', padx=20, pady=2)
        
        Label(
            radio_frame,
            text="   → Export without coordinates (faster)",
            font=("Arial", 8),
            fg="#555",
            bg="#f0f8ff"
        ).pack(anchor='w', padx=40, pady=(0, 8))

        ttk.Separator(parent, orient='horizontal').pack(fill=X, pady=10, padx=20)

        # Extraction Status
        self.status_label = Label(parent, textvariable=self.extraction_status, 
                                 fg="blue", font=("Arial", 10), wraplength=600)
        self.status_label.pack(pady=10)

        # Extract Button (larger and more prominent)
        self.extract_btn = Button(
            parent, 
            text="🚀 Start Extraction", 
            command=self.start_extraction,
            bg="#4CAF50",
            fg="white",
            font=("Arial", 12, "bold"),
            padx=40,
            pady=15,
            cursor="hand2"
        )
        self.extract_btn.pack(pady=20)
        
        # Add some bottom padding
        Label(parent, text="", height=2).pack()

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
        
        # Enable "Load Groups" button and geocoding for Ledger type
        if master_type == "Ledger":
            self.load_groups_btn.config(state='normal')
            # Enable all radio buttons
            self.geocode_radio1.config(state='normal')
            self.geocode_radio2.config(state='normal')
            self.geocode_radio3.config(state='normal')
        else:
            self.load_groups_btn.config(state='disabled')
            # Disable all radio buttons
            self.geocode_radio1.config(state='disabled')
            self.geocode_radio2.config(state='disabled')
            self.geocode_radio3.config(state='disabled')
            self.geocode_method.set(3)  # Set to "No Geocoding"

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
            self.load_groups_btn.config(state='normal', text="🔄 Load Groups")

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
        self.extract_btn.config(state='disabled', text="⏳ Extracting...")

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
                
                # Enhanced Geocoding with Google Places
                geocode_option = self.geocode_method.get()
                
                if geocode_option == 1:
                    # Enhanced method
                    self._update_status("🌍 Enhanced geocoding (Google Places + Address)...")
                    print(f"DEBUG: Starting ENHANCED geocoding for {len(df)} ledgers...")
                    
                    df = geocode_dataframe(
                        df, 
                        address_col="address", 
                        name_col="name",
                        max_workers=4,
                        use_enhanced=True
                    )
                    
                    # Count success by source
                    if 'location_source' in df.columns:
                        places_count = len(df[df['location_source'] == 'google_places'])
                        geocoded_count = len(df[df['location_source'] == 'geocoded'])
                        not_found = len(df[df['location_source'] == 'not_found'])
                        
                        print(f"DEBUG: Enhanced Geocoding complete:")
                        print(f"  - Google Places: {places_count}")
                        print(f"  - Address Geocoding: {geocoded_count}")
                        print(f"  - Not Found: {not_found}")
                        
                        self._update_status(
                            f"✅ Enhanced: {places_count} via Places, "
                            f"{geocoded_count} via address, {not_found} not found"
                        )
                
                elif geocode_option == 2:
                    # Basic method
                    self._update_status("📍 Basic geocoding (Address only)...")
                    print(f"DEBUG: Starting BASIC geocoding for {len(df)} ledgers...")
                    
                    df = geocode_dataframe(
                        df, 
                        address_col="address",
                        max_workers=4,
                        use_enhanced=False
                    )
                    
                    # Count success
                    if 'location_source' in df.columns:
                        geocoded_count = len(df[df['location_source'] == 'geocoded'])
                        not_found = len(df[df['location_source'] == 'not_found'])
                        
                        print(f"DEBUG: Basic Geocoding complete:")
                        print(f"  - Address Geocoding: {geocoded_count}")
                        print(f"  - Not Found: {not_found}")
                        
                        self._update_status(
                            f"✅ Basic: {geocoded_count} geocoded, {not_found} not found"
                        )
                
                else:
                    # No geocoding
                    print(f"DEBUG: Skipping geocoding (user selected 'No Geocoding')")
                    self._update_status("⏭️ Skipped geocoding")
                    
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
            
            # Build success message
            success_msg = f"Exported {len(df)} {master_type} records\n"
            success_msg += f"Categories: {', '.join(categories)}\n"
            
            # Add geocoding stats if applicable
            if master_type == "Ledger" and 'location_source' in df.columns:
                geocode_option = self.geocode_method.get()
                
                if geocode_option == 1:
                    # Enhanced stats
                    places_count = len(df[df['location_source'] == 'google_places'])
                    geocoded_count = len(df[df['location_source'] == 'geocoded'])
                    success_msg += f"\n🚀 Enhanced Geocoding Results:\n"
                    success_msg += f"  • Google Places: {places_count}\n"
                    success_msg += f"  • Address Geocoding: {geocoded_count}\n"
                elif geocode_option == 2:
                    # Basic stats
                    geocoded_count = len(df[df['location_source'] == 'geocoded'])
                    success_msg += f"\n📍 Basic Geocoding Results:\n"
                    success_msg += f"  • Address Geocoding: {geocoded_count}\n"
            
            success_msg += f"\nSaved to {filename}"
            
            self.root.after(0, lambda: messagebox.showinfo("Success", success_msg))

        except Exception as e:
            print(f"DEBUG ERROR: {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
            
            self._update_status("❌ Extraction failed")
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.root.after(0, lambda: messagebox.showerror("Error", error_msg))
        
        finally:
            self.root.after(0, lambda: self.extract_btn.config(state='normal', text="🚀 Start Extraction"))

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