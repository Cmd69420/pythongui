from tkinter import Tk, Label, Button, StringVar, ttk, messagebox, Entry, Checkbutton, BooleanVar, Frame, Listbox, Scrollbar, MULTIPLE, END, VERTICAL, BOTH, LEFT, RIGHT, TOP, BOTTOM, X, Y, Canvas, Radiobutton, IntVar
from tally_client import TallyClient
from server_client import server_health_check
from config import TALLY_URL, SERVER_HEALTH_URL
from parser import parse_ledgers
from geocoder import geocode_dataframe
from backend_uploader import BackendUploader, prepare_client_for_upload, MIDDLEWARE_TOKEN
import threading
import time
import json
import os
import hashlib
from datetime import datetime

OUTPUT_FILE = "tally_export.csv"
CACHE_FILE = "tally_cache.json"  # Stores previous sync data

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
        self.root.title("Tally Middleware - Smart Auto-Sync")
        self.root.geometry("750x850")
        
        self.root.minsize(700, 750)

        self.tally = TallyClient(TALLY_URL)
        self.backend_uploader = BackendUploader(SERVER_HEALTH_URL, MIDDLEWARE_TOKEN)
        
        self.selected_company_id = None
        self.selected_company_name = None

        # Status variables
        self.tally_status = StringVar(value="Checking...")
        self.server_status = StringVar(value="Checking...")
        self.company_var = StringVar()
        self.master_type_var = StringVar()
        self.do_geocode = BooleanVar(value=True)
        self.geocode_method = IntVar(value=1)
        self.upload_to_backend = BooleanVar(value=False)
        self.extraction_status = StringVar(value="")
        self.is_company_secured = False

        # 🆕 AUTO-SYNC VARIABLES
        self.auto_sync_enabled = BooleanVar(value=False)
        self.sync_interval = IntVar(value=1)  # minutes
        self.auto_sync_thread = None
        self.stop_auto_sync = threading.Event()
        self.last_sync_time = StringVar(value="Never")
        self.sync_stats = StringVar(value="No syncs yet")

        self.build_scrollable_ui()
        self.refresh_status()

    def build_scrollable_ui(self):
        main_container = Frame(self.root)
        main_container.pack(fill=BOTH, expand=True)
        
        self.canvas = Canvas(main_container, highlightthickness=0)
        self.canvas.pack(side=LEFT, fill=BOTH, expand=True)
        
        scrollbar = Scrollbar(main_container, orient=VERTICAL, command=self.canvas.yview)
        scrollbar.pack(side=RIGHT, fill=Y)
        
        self.canvas.configure(yscrollcommand=scrollbar.set)
        
        self.scrollable_frame = Frame(self.canvas)
        self.canvas_frame = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        
        self.scrollable_frame.bind("<Configure>", self.on_frame_configure)
        self.canvas.bind_all("<MouseWheel>", self.on_mousewheel)
        self.canvas.bind("<Configure>", self.on_canvas_configure)
        
        self.build_ui(self.scrollable_frame)
    
    def on_frame_configure(self, event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
    
    def on_canvas_configure(self, event):
        canvas_width = event.width
        self.canvas.itemconfig(self.canvas_frame, width=canvas_width)
    
    def on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def build_ui(self, parent):
        # Header
        Label(parent, text="Tally Middleware - Smart Auto-Sync", 
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
        
        self.load_groups_btn = Button(
            master_type_frame,
            text="🔄 Load Groups",
            command=self.load_groups_from_tally,
            state='disabled',
            padx=10,
            pady=5
        )
        self.load_groups_btn.pack(side=LEFT, padx=5)

        # Sub-category selection
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

        # Geocoding Options
        geocode_frame = Frame(parent, relief="solid", borderwidth=2, bg="#f0f8ff")
        geocode_frame.pack(pady=10, padx=30, fill=X)
        
        Label(
            geocode_frame,
            text="🌍 Geocoding Options",
            font=("Arial", 11, "bold"),
            bg="#f0f8ff"
        ).pack(pady=8)
        
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

        ttk.Separator(parent, orient='horizontal').pack(fill=X, pady=10, padx=20)

        # Backend Upload Section
        upload_frame = Frame(parent, relief="solid", borderwidth=2, bg="#fff5e6")
        upload_frame.pack(pady=10, padx=30, fill=X)
        
        Label(
            upload_frame,
            text="📤 Backend Upload Options",
            font=("Arial", 11, "bold"),
            bg="#fff5e6"
        ).pack(pady=8)
        
        self.upload_checkbox = Checkbutton(
            upload_frame,
            text="Upload to Backend Server After Export",
            variable=self.upload_to_backend,
            font=("Arial", 10),
            bg="#fff5e6",
            state='disabled'
        )
        self.upload_checkbox.pack(anchor='w', padx=20, pady=5)

        ttk.Separator(parent, orient='horizontal').pack(fill=X, pady=10, padx=20)

        # 🆕 AUTO-SYNC SECTION
        auto_sync_frame = Frame(parent, relief="solid", borderwidth=2, bg="#e6f7ff")
        auto_sync_frame.pack(pady=10, padx=30, fill=X)
        
        Label(
            auto_sync_frame,
            text="🔄 Auto-Sync (Cron Mode)",
            font=("Arial", 11, "bold"),
            bg="#e6f7ff"
        ).pack(pady=8)
        
        self.auto_sync_checkbox = Checkbutton(
            auto_sync_frame,
            text="Enable Auto-Sync (Only New/Changed Data)",
            variable=self.auto_sync_enabled,
            font=("Arial", 10),
            bg="#e6f7ff",
            command=self.toggle_auto_sync
        )
        self.auto_sync_checkbox.pack(anchor='w', padx=20, pady=5)
        
        interval_frame = Frame(auto_sync_frame, bg="#e6f7ff")
        interval_frame.pack(pady=5)
        
        Label(interval_frame, text="Sync Every:", bg="#e6f7ff").pack(side=LEFT, padx=5)
        
        self.interval_spinbox = ttk.Spinbox(
            interval_frame,
            from_=1,
            to=60,
            textvariable=self.sync_interval,
            width=5
        )
        self.interval_spinbox.pack(side=LEFT)
        
        Label(interval_frame, text="minute(s)", bg="#e6f7ff").pack(side=LEFT, padx=5)
        
        Label(
            auto_sync_frame,
            textvariable=self.last_sync_time,
            font=("Arial", 9),
            fg="#666",
            bg="#e6f7ff"
        ).pack(pady=2)
        
        Label(
            auto_sync_frame,
            textvariable=self.sync_stats,
            font=("Arial", 9),
            fg="#666",
            bg="#e6f7ff"
        ).pack(pady=2)

        ttk.Separator(parent, orient='horizontal').pack(fill=X, pady=10, padx=20)

        # Extraction Status
        self.status_label = Label(parent, textvariable=self.extraction_status, 
                                 fg="blue", font=("Arial", 10), wraplength=600)
        self.status_label.pack(pady=10)

        # Extract Button
        self.extract_btn = Button(
            parent, 
            text="🚀 Manual Sync Now", 
            command=self.start_extraction,
            bg="#4CAF50",
            fg="white",
            font=("Arial", 12, "bold"),
            padx=40,
            pady=15,
            cursor="hand2"
        )
        self.extract_btn.pack(pady=20)
        
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
                self.on_company_selected()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load companies\n{e}")

    def on_company_selected(self, event=None):
        company = self.company_var.get()
        if not company:
            return
        
        self.selected_company_name = company
        
        try:
            self.extraction_status.set("Checking company security...")
            self.root.update()
            
            self.is_company_secured = self.tally.check_company_security(company)
            
            if self.is_company_secured:
                self.extraction_status.set("🔒 This company requires username and password")
                self.user_entry.config(bg="#fff9e6")
                self.pass_entry.config(bg="#fff9e6")
            else:
                self.extraction_status.set("🔓 This company is not password protected")
                self.user_entry.config(bg="white")
                self.pass_entry.config(bg="white")
                
        except Exception as e:
            print(f"Error checking security: {e}")
            self.extraction_status.set("")

    def on_master_type_change(self, event=None):
        master_type = self.master_type_var.get()
        
        self.category_listbox.delete(0, END)
        
        if master_type in MASTER_TYPES:
            categories = MASTER_TYPES[master_type]
            for cat in categories:
                self.category_listbox.insert(END, cat)
        
        if master_type == "Ledger":
            self.load_groups_btn.config(state='normal')
            self.geocode_radio1.config(state='normal')
            self.geocode_radio2.config(state='normal')
            self.geocode_radio3.config(state='normal')
            self.upload_checkbox.config(state='normal')
        else:
            self.load_groups_btn.config(state='disabled')
            self.geocode_radio1.config(state='disabled')
            self.geocode_radio2.config(state='disabled')
            self.geocode_radio3.config(state='disabled')
            self.geocode_method.set(3)
            self.upload_checkbox.config(state='disabled')
            self.upload_to_backend.set(False)

    def load_groups_from_tally(self):
        company = self.company_var.get()
        
        if not company:
            messagebox.showwarning("Select Company", "Please select a company first")
            return
        
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
            
            groups = self.tally.fetch_groups(company, user, pw)
            
            self.category_listbox.delete(0, END)
            self.category_listbox.insert(END, "All Ledgers")
            for group in groups:
                self.category_listbox.insert(END, group)
            
            self.extraction_status.set(f"✅ Loaded {len(groups)} groups from Tally")
            messagebox.showinfo("Success", f"Loaded {len(groups)} groups from Tally!")
            
        except Exception as e:
            error_msg = str(e)
            if "authentication" in error_msg.lower() or "login" in error_msg.lower():
                messagebox.showerror(
                    "Authentication Required", 
                    "Invalid credentials. Please check username/password."
                )
            else:
                messagebox.showerror("Error", f"Failed to load groups:\n{error_msg}")
            self.extraction_status.set("❌ Using default groups")
        finally:
            self.load_groups_btn.config(state='normal', text="🔄 Load Groups")

    def select_all_categories(self):
        self.category_listbox.select_set(0, END)

    def deselect_all_categories(self):
        self.category_listbox.select_clear(0, END)

    def get_selected_categories(self):
        selected_indices = self.category_listbox.curselection()
        return [self.category_listbox.get(i) for i in selected_indices]

    # 🆕 AUTO-SYNC FUNCTIONS
    def toggle_auto_sync(self):
        if self.auto_sync_enabled.get():
            self.start_auto_sync()
        else:
            self.stop_auto_sync_thread()

    def start_auto_sync(self):
        # Validate settings
        if not self.company_var.get():
            messagebox.showwarning("Setup Required", "Please select a company first")
            self.auto_sync_enabled.set(False)
            return
        
        if not self.master_type_var.get():
            messagebox.showwarning("Setup Required", "Please select a master type")
            self.auto_sync_enabled.set(False)
            return
        
        if not self.selected_company_id:
            from tkinter import simpledialog
            company_id = simpledialog.askstring(
                "Company ID Required",
                f"Enter Company ID for '{self.company_var.get()}':",
                parent=self.root
            )
            
            if not company_id:
                messagebox.showwarning("Setup Required", "Company ID is required for auto-sync")
                self.auto_sync_enabled.set(False)
                return
            
            self.selected_company_id = company_id.strip()
        
        # Start background thread
        self.stop_auto_sync.clear()
        self.auto_sync_thread = threading.Thread(target=self._auto_sync_loop, daemon=True)
        self.auto_sync_thread.start()
        
        print(f"✅ Auto-sync started: Every {self.sync_interval.get()} minute(s)")
        self.extraction_status.set(f"🔄 Auto-sync enabled: Every {self.sync_interval.get()} min")

    def stop_auto_sync_thread(self):
        self.stop_auto_sync.set()
        if self.auto_sync_thread:
            self.auto_sync_thread.join(timeout=2)
        
        print("⏹️ Auto-sync stopped")
        self.extraction_status.set("⏹️ Auto-sync disabled")

    def _auto_sync_loop(self):
        """Background thread that runs sync at intervals"""
        while not self.stop_auto_sync.is_set():
            try:
                print(f"\n🔄 Auto-sync triggered at {datetime.now().strftime('%H:%M:%S')}")
                
                # Run smart sync (only new/changed)
                self._smart_sync()
                
                # Update last sync time
                self.last_sync_time.set(f"Last sync: {datetime.now().strftime('%H:%M:%S')}")
                
            except Exception as e:
                print(f"❌ Auto-sync error: {e}")
                import traceback
                traceback.print_exc()
            
            # Wait for interval (checking stop flag every second)
            interval_seconds = self.sync_interval.get() * 60
            for _ in range(interval_seconds):
                if self.stop_auto_sync.is_set():
                    break
                time.sleep(1)

    def _smart_sync(self):
        """Smart sync: Only upload NEW or CHANGED clients"""
        company = self.company_var.get()
        user = self.user_entry.get().strip()
        pw = self.pass_entry.get().strip()
        master_type = self.master_type_var.get()
        categories = self.get_selected_categories()
        
        if not categories:
            categories = ["All Ledgers"]
        
        print(f"🔍 Fetching current data from Tally...")
        
        # Fetch current data from Tally
        if master_type == "Ledger":
            xml = self.tally.fetch_ledgers(company, user, pw)
            df = parse_ledgers(xml)
            
            if categories and "All Ledgers" not in categories:
                categories_lower = [c.lower() for c in categories]
                df = df[df['parent'].str.lower().isin(categories_lower)]
            
            # Geocode if needed
            geocode_option = self.geocode_method.get()
            if geocode_option in [1, 2]:
                df = geocode_dataframe(
                    df, 
                    address_col="address",
                    name_col="name" if geocode_option == 1 else None,
                    max_workers=4,
                    use_enhanced=(geocode_option == 1)
                )
        else:
            print(f"⚠️ Auto-sync only supports Ledger type currently")
            return
        
        # Convert to dict for comparison
        current_data = df.to_dict('records')
        
        # Load previous cache
        previous_data = self._load_cache()
        
        # Find NEW and CHANGED clients
        new_clients, changed_clients = self._detect_changes(current_data, previous_data)
        
        total_to_upload = len(new_clients) + len(changed_clients)
        
        if total_to_upload == 0:
            print(f"✅ No changes detected - skipping upload")
            self.sync_stats.set(f"No changes detected")
            return
        
        print(f"\n📊 Change Detection:")
        print(f"   New clients: {len(new_clients)}")
        print(f"   Changed clients: {len(changed_clients)}")
        print(f"   Total to upload: {total_to_upload}")
        
        # Prepare clients for upload
        clients_to_upload = new_clients + changed_clients
        clients_data = [prepare_client_for_upload(c) for c in clients_to_upload]
        
        # Upload to backend
        if self.selected_company_id:
            try:
                result = self.backend_uploader.upload_clients(
                    clients_data,
                    company_id=self.selected_company_id,
                    batch_size=100
                )
                
                summary = result.get('summary', {})
                self.sync_stats.set(
                    f"Uploaded: {len(new_clients)} new, {len(changed_clients)} changed"
                )
                
                print(f"✅ Auto-sync upload complete")
                
                # Save current data as cache for next comparison
                self._save_cache(current_data)
                
            except Exception as e:
                print(f"❌ Upload failed: {e}")
                self.sync_stats.set(f"Upload failed: {str(e)[:50]}...")

    def _load_cache(self):
        """Load previous sync data from cache file"""
        cache_file = f"{CACHE_FILE}.{self.selected_company_name}.json"
        
        if not os.path.exists(cache_file):
            return {}
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Failed to load cache: {e}")
            return {}

    def _save_cache(self, data):
        """Save current data to cache for next comparison"""
        cache_file = f"{CACHE_FILE}.{self.selected_company_name}.json"
        
        # Convert to hashable format
        cache_data = {}
        for client in data:
            key = client.get('guid') or client.get('name', 'unknown')
            cache_data[key] = self._client_hash(client)
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
            print(f"💾 Cache saved: {len(cache_data)} clients")
        except Exception as e:
            print(f"⚠️ Failed to save cache: {e}")

    def _client_hash(self, client):
        """Generate hash of client data to detect changes"""
        # Use important fields for change detection
        key_fields = ['name', 'address', 'phone', 'email', 'pincode', 'parent']
        
        data_str = '|'.join([
            str(client.get(field, '')) for field in key_fields
        ])
        
        return hashlib.md5(data_str.encode()).hexdigest()

    def _detect_changes(self, current_data, previous_cache):
        """Compare current data with previous cache to find new/changed clients"""
        new_clients = []
        changed_clients = []
        
        for client in current_data:
            key = client.get('guid') or client.get('name', 'unknown')
            current_hash = self._client_hash(client)
            
            if key not in previous_cache:
                # New client
                new_clients.append(client)
            elif previous_cache[key] != current_hash:
                # Changed client
                changed_clients.append(client)
        
        return new_clients, changed_clients

    def start_extraction(self):
        company = self.company_var.get()
        user = self.user_entry.get().strip()
        pw = self.pass_entry.get().strip()
        master_type = self.master_type_var.get()
        selected_categories = self.get_selected_categories()

        if not company:
            messagebox.showwarning("Select Company", "Please select a company")
            return

        if not master_type:
            messagebox.showwarning("Select Master Type", "Please select a master type")
            return

        if not selected_categories:
            messagebox.showwarning("Select Categories", "Please select at least one category")
            return

        if self.is_company_secured and (not user or not pw):
            messagebox.showerror("Credentials Required", "Username and password required")
            return

        # Ask for company ID if uploading
        if self.upload_to_backend.get():
            if not self.selected_company_id:
                from tkinter import simpledialog

                company_id = simpledialog.askstring(
                    "Company ID Required",
                    f"Enter Company ID for '{company}':",
                    parent=self.root
                )

                if not company_id or not company_id.strip():
                    messagebox.showwarning(
                        "Upload Cancelled", 
                        "Company ID is required for backend upload.")
                    self.upload_to_backend.set(False)
                    # Don't return - continue with export only
                else:
                    self.selected_company_id = company_id.strip()
                    print(f"✅ Company ID captured: {self.selected_company_id}")

        self.extract_btn.config(state='disabled', text="⏳ Extracting...")

        thread = threading.Thread(
            target=self._run_extraction,
            args=(company, user, pw, master_type, selected_categories),
            daemon=True
        )
        thread.start()

    def _run_extraction(self, company, user, pw, master_type, categories):
        try:
            print(f"DEBUG: Starting extraction for {master_type}")
            print(f"DEBUG: Categories: {categories}")
            
            self._update_status(f"Fetching {master_type} data from Tally...")

            if master_type == "Ledger":
                print(f"DEBUG: Fetching all ledgers...")
                xml = self.tally.fetch_ledgers(company, user, pw)
                print(f"DEBUG: Received XML length: {len(xml)}")
                
                self._update_status("Parsing ledger data...")
                df = parse_ledgers(xml)
                print(f"DEBUG: Parsed {len(df)} ledgers")
                
                self._update_status("Filtering selected categories...")
                if categories and "All Ledgers" not in categories:
                    categories_lower = [c.lower() for c in categories]
                    df = df[df['parent'].str.lower().isin(categories_lower)]
                    print(f"DEBUG: Filtered to {len(df)} ledgers from categories: {categories}")
                
                geocode_option = self.geocode_method.get()
                
                if geocode_option == 1:
                    self._update_status("🌍 Enhanced geocoding (Google Places + Address)...")
                    print(f"DEBUG: Starting ENHANCED geocoding for {len(df)} ledgers...")
                    
                    df = geocode_dataframe(
                        df, 
                        address_col="address", 
                        name_col="name",
                        max_workers=4,
                        use_enhanced=True
                    )
                    
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
                    self._update_status("📍 Basic geocoding (Address only)...")
                    print(f"DEBUG: Starting BASIC geocoding for {len(df)} ledgers...")
                    
                    df = geocode_dataframe(
                        df, 
                        address_col="address",
                        max_workers=4,
                        use_enhanced=False
                    )
                    
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
                    print(f"DEBUG: Skipping geocoding (user selected 'No Geocoding')")
                    self._update_status("⏭️ Skipped geocoding")
                
                # ======= BACKEND UPLOAD LOGIC =======
                upload_msg = ""
                
                if self.upload_to_backend.get() and geocode_option != 3 and self.selected_company_id:
                    self._update_status("📤 Uploading to backend server...")
                    print(f"DEBUG: Preparing to upload {len(df)} ledgers to backend...")
                    print(f"DEBUG: Using company ID: {self.selected_company_id}")
                    
                    try:
                        # Convert DataFrame rows to backend format
                        clients_data = []
                        for _, row in df.iterrows():
                            client = prepare_client_for_upload(row.to_dict())
                            clients_data.append(client)
                        
                        print(f"DEBUG: Converted {len(clients_data)} clients for upload")
                        
                        # Upload to backend
                        result = self.backend_uploader.upload_clients(
                            clients_data, 
                            company_id=self.selected_company_id,
                            batch_size=100
                        )
                        
                        summary = result.get('summary', {})
                        upload_msg = (
                            f"\n\n📤 Backend Upload Complete:\n"
                            f"  • New: {summary.get('new', 0)}\n"
                            f"  • Updated: {summary.get('updated', 0)}\n"
                            f"  • Failed: {summary.get('failed', 0)}\n"
                        )
                        
                        self._update_status("✅ Upload complete!")
                        print(f"DEBUG: Upload successful")
                        print(upload_msg)
                        
                        # 🆕 Save cache after successful upload
                        current_data = df.to_dict('records')
                        self._save_cache(current_data)
                        
                    except Exception as upload_error:
                        upload_msg = f"\n\n❌ Backend Upload Failed:\n{str(upload_error)}"
                        print(f"DEBUG: Upload error: {upload_error}")
                        import traceback
                        traceback.print_exc()
                        self._update_status("❌ Upload failed")
                    
            else:
                print(f"DEBUG: Fetching {master_type} masters...")
                xml = self.tally.fetch_masters(company, user, pw, master_type)
                print(f"DEBUG: Received XML length: {len(xml)}")
                
                self._update_status(f"Parsing {master_type} data...")
                df = self._parse_generic_master(xml, master_type)
                print(f"DEBUG: Parsed {len(df)} records")
                upload_msg = ""

            # Save to CSV
            filename = f"tally_{master_type.lower().replace(' ', '_')}_{company[:20]}.csv"
            df.to_csv(filename, index=False)
            print(f"DEBUG: Saved to {filename}")

            self._update_status(f"✅ Saved successfully to {filename}")
            
            # Build success message
            success_msg = f"Exported {len(df)} {master_type} records\n"
            success_msg += f"Categories: {', '.join(categories)}\n"
            
            if master_type == "Ledger" and 'location_source' in df.columns:
                geocode_option = self.geocode_method.get()
                
                if geocode_option == 1:
                    places_count = len(df[df['location_source'] == 'google_places'])
                    geocoded_count = len(df[df['location_source'] == 'geocoded'])
                    success_msg += f"\n🚀 Enhanced Geocoding Results:\n"
                    success_msg += f"  • Google Places: {places_count}\n"
                    success_msg += f"  • Address Geocoding: {geocoded_count}\n"
                elif geocode_option == 2:
                    geocoded_count = len(df[df['location_source'] == 'geocoded'])
                    success_msg += f"\n📍 Basic Geocoding Results:\n"
                    success_msg += f"  • Address Geocoding: {geocoded_count}\n"
            
            success_msg += upload_msg
            success_msg += f"\n\nSaved to {filename}"
            
            self.root.after(0, lambda: messagebox.showinfo("Success", success_msg))

        except Exception as e:
            print(f"DEBUG ERROR: {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
            
            self._update_status("❌ Extraction failed")
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.root.after(0, lambda: messagebox.showerror("Error", error_msg))
        
        finally:
            self.root.after(0, lambda: self.extract_btn.config(state='normal', text="🚀 Manual Sync Now"))

    def _parse_generic_master(self, xml, master_type):
        import pandas as pd
        from lxml import etree
        
        parser = etree.XMLParser(recover=True, huge_tree=True)
        root = etree.fromstring(xml.encode(), parser)
        
        rows = []
        for elem in root.findall(f".//{master_type.upper().replace(' ', '')}"):
            rows.append({
                "name": elem.get("NAME", ""),
                "guid": elem.findtext("GUID", "")
            })
        
        return pd.DataFrame(rows)

    def _update_status(self, message):
        self.root.after(0, lambda: self.extraction_status.set(message))


if __name__ == "__main__":
    root = Tk()
    app = MiddlewareApp(root)
    
    # Handle window close event to stop auto-sync
    def on_closing():
        if app.auto_sync_enabled.get():
            app.stop_auto_sync_thread()
        root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()