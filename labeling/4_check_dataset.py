import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import os
import argparse
import sys
from datetime import datetime
import json
import os.path
from pathlib import Path

# Điều chỉnh đường dẫn import
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

# Thêm thư mục cha vào path để import config
from utils.file_utils import save_excel_file
import config

def parse_args():
    parser = argparse.ArgumentParser(description='Check and audit dataset')
    parser.add_argument('--version', '-v', help='Version to process (e.g., v1, v2)')
    parser.add_argument('--file', '-f', help='Specific file to check (overrides version)')
    parser.add_argument('--input', '-i', default=None, 
                        help='Input filename (default: version-specific)')
    return parser.parse_args()

class ColumnSelector:
    def __init__(self, root, df):
        self.root = root
        self.df = df
        self.selected_columns = []
        self.result = None
        
        # Create dialog window
        self.dialog = tk.Toplevel(root)
        self.dialog.title("Select Columns")
        self.dialog.geometry("500x400")
        self.dialog.grab_set()  # Make dialog modal
        
        # Main frame
        main_frame = ttk.Frame(self.dialog)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Instruction label
        ttk.Label(main_frame, text="Select columns to include in the dataset:").pack(pady=(0, 10))
        
        # Column selection frame with scrollbar
        list_frame = ttk.Frame(main_frame)
        list_frame.pack(fill=tk.BOTH, expand=True)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Listbox with checkboxes
        self.listbox = tk.Listbox(list_frame, selectmode=tk.MULTIPLE, yscrollcommand=scrollbar.set)
        self.listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.listbox.yview)
        
        # Add columns to listbox
        for col in df.columns:
            self.listbox.insert(tk.END, col)
        
        # Pre-select important columns
        important_cols = ['post_id', 'post_raw', 'summary', 'comment_id', 'comment_raw', 'created_date', 'platform', 'label']
        for i, col in enumerate(df.columns):
            if col in important_cols:
                self.listbox.selection_set(i)
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(button_frame, text="Select All", command=self.select_all).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="Clear All", command=self.clear_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="OK", command=self.ok_clicked).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(button_frame, text="Cancel", command=self.cancel_clicked).pack(side=tk.RIGHT)
        
        # Wait for user action
        self.dialog.wait_window()
    
    def select_all(self):
        self.listbox.selection_set(0, tk.END)
    
    def clear_all(self):
        self.listbox.selection_clear(0, tk.END)
    
    def ok_clicked(self):
        selected_indices = self.listbox.curselection()
        self.selected_columns = [self.df.columns[i] for i in selected_indices]
        self.result = self.df[self.selected_columns].copy() if self.selected_columns else None
        self.dialog.destroy()
    
    def cancel_clicked(self):
        self.result = None
        self.dialog.destroy()

class FastCommentAuditor:
    def __init__(self, root, file_path):
        self.root = root
        self.root.title("Fast Comment Auditor")
        self.root.geometry("1400x800")
        
        # Load dataset with column selection
        self.file_path = file_path
        self.version = self.extract_version_from_path(file_path)
        
        # Get version-specific checkpoint filename
        checkpoint_filename = config.get_version_filename(self.version, "audit_checkpoint")
        self.checkpoint_path = os.path.join(os.path.dirname(file_path), checkpoint_filename)
        
        if not self.load_dataset():
            self.root.destroy()
            return
        
        # Set up main frame
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create filter controls
        filter_frame = ttk.Frame(main_frame)
        filter_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Filter options - Updated to use text labels
        ttk.Label(filter_frame, text="Filter:").grid(row=0, column=0, padx=5)
        self.filter_var = tk.StringVar(value="All")
        ttk.Radiobutton(filter_frame, text="All", variable=self.filter_var, value="All").grid(row=0, column=1, padx=5)
        ttk.Radiobutton(filter_frame, text="PHẢN ĐỘNG", variable=self.filter_var, value="PHAN_DONG").grid(row=0, column=2, padx=5)
        ttk.Radiobutton(filter_frame, text="KHÔNG PHẢN ĐỘNG", variable=self.filter_var, value="KHONG_PHAN_DONG").grid(row=0, column=3, padx=5)
        ttk.Radiobutton(filter_frame, text="KHÔNG LIÊN QUAN", variable=self.filter_var, value="KHONG_LIEN_QUAN").grid(row=0, column=4, padx=5)
        ttk.Radiobutton(filter_frame, text="Short (<5 words)", variable=self.filter_var, value="short").grid(row=0, column=5, padx=5)
        ttk.Button(filter_frame, text="Apply Filter", command=self.apply_filter).grid(row=0, column=6, padx=10)
        
        # Search box
        ttk.Label(filter_frame, text="Search:").grid(row=0, column=7, padx=(20, 5))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(filter_frame, textvariable=self.search_var, width=20)
        search_entry.grid(row=0, column=8, padx=5)
        ttk.Button(filter_frame, text="Search", command=self.apply_search).grid(row=0, column=9, padx=5)
        
        # Quick edit controls - Updated button texts
        edit_frame = ttk.Frame(main_frame)
        edit_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(edit_frame, text="Set selected comments to:").grid(row=0, column=0, padx=5)
        ttk.Button(edit_frame, text="PHẢN ĐỘNG (1)", command=lambda: self.batch_update_labels("PHAN_DONG")).grid(row=0, column=1, padx=5)
        ttk.Button(edit_frame, text="KHÔNG PHẢN ĐỘNG (2)", command=lambda: self.batch_update_labels("KHONG_PHAN_DONG")).grid(row=0, column=2, padx=5)
        ttk.Button(edit_frame, text="KHÔNG LIÊN QUAN (3)", command=lambda: self.batch_update_labels("KHONG_LIEN_QUAN")).grid(row=0, column=3, padx=5)
        ttk.Button(edit_frame, text="Mark for Deletion (4)", command=self.mark_for_deletion).grid(row=0, column=4, padx=5)
        ttk.Button(edit_frame, text="Unmark (5)", command=self.unmark_deletion).grid(row=0, column=5, padx=5)

        # Export buttons - Updated names and descriptions
        ttk.Button(edit_frame, text="Save Progress (Keep Flags)", command=self.save_progress).grid(row=0, column=6, padx=(30, 5))
        ttk.Button(edit_frame, text="Export Final (Remove Deleted)", command=self.export_final).grid(row=0, column=7, padx=(5, 20))

        # Statistics label
        self.stats_var = tk.StringVar()
        ttk.Label(edit_frame, textvariable=self.stats_var).grid(row=0, column=8, padx=(20, 5))
        
        # Create comment view with treeview
        self.create_comment_view(main_frame)
        
        # Show post details when a comment is selected - Updated to show both post_raw and summary
        details_frame = ttk.Frame(main_frame)
        details_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Post raw frame
        post_frame = ttk.LabelFrame(details_frame, text="Related Post")
        post_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        self.post_text = tk.Text(post_frame, height=8, wrap=tk.WORD)
        post_scrollbar = ttk.Scrollbar(post_frame, orient="vertical", command=self.post_text.yview)
        self.post_text.configure(yscrollcommand=post_scrollbar.set)
        
        self.post_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        post_scrollbar.pack(side=tk.RIGHT, fill=tk.Y, pady=5)
        
        # Summary frame
        summary_frame = ttk.LabelFrame(details_frame, text="Summary")
        summary_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))
        
        self.summary_text = tk.Text(summary_frame, height=8, wrap=tk.WORD)
        summary_scrollbar = ttk.Scrollbar(summary_frame, orient="vertical", command=self.summary_text.yview)
        self.summary_text.configure(yscrollcommand=summary_scrollbar.set)
        
        self.summary_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        summary_scrollbar.pack(side=tk.RIGHT, fill=tk.Y, pady=5)
        
        # Update stats
        self.update_stats()
        
        # Bind keyboard shortcuts - Updated to use text values
        self.root.bind("1", lambda event: self.batch_update_labels("PHAN_DONG"))
        self.root.bind("2", lambda event: self.batch_update_labels("KHONG_PHAN_DONG"))
        self.root.bind("3", lambda event: self.batch_update_labels("KHONG_LIEN_QUAN"))
        self.root.bind("4", lambda event: self.mark_for_deletion())
        self.root.bind("5", lambda event: self.unmark_deletion())
        self.root.bind("d", lambda event: self.mark_for_deletion())
        self.root.bind("<Delete>", lambda event: self.mark_for_deletion())
        self.root.bind("u", lambda event: self.unmark_deletion())
        
        # Load checkpoint if it exists
        self.load_checkpoint()
        
        # Add autosave timer (save checkpoint every 5 minutes)
        self.root.after(300000, self.auto_save_checkpoint)
        
        # Bind window close event to save checkpoint
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def extract_version_from_path(self, file_path):
        """Extract version from file path"""
        path_parts = Path(file_path).parts
        for part in path_parts:
            if part.startswith('v') and part[1:].isdigit():
                return part
        return "v2"  # Default to v2 instead of "default"
    
    def load_dataset(self):
        try:
            # Load full dataset first
            full_df = pd.read_excel(self.file_path)
            
            # Show column selector
            selector = ColumnSelector(self.root, full_df)
            
            if selector.result is None:
                messagebox.showinfo("Cancelled", "No columns selected. Exiting...")
                return False
            
            self.df = selector.result
            
            # Add a delete flag column if not present
            if 'delete_flag' not in self.df.columns:
                self.df['delete_flag'] = False
            
            # Handle label column - convert to text labels if numeric
            if 'label' in self.df.columns:
                # Convert numeric labels to text labels
                label_mapping = {1: "PHAN_DONG", 2: "KHONG_PHAN_DONG", 3: "KHONG_LIEN_QUAN"}
                self.df['label'] = self.df['label'].apply(lambda x: label_mapping.get(x, str(x)))
                # Fill NaN values with default
                self.df['label'] = self.df['label'].fillna("KHONG_LIEN_QUAN")
            else:
                # Add label column if not present
                self.df['label'] = "KHONG_LIEN_QUAN"
            
            messagebox.showinfo("Dataset Loaded", 
                              f"Loaded {len(self.df)} records with {len(self.df.columns)} columns")
            return True
            
        except Exception as e:
            messagebox.showerror("Error", f"Could not load file: {str(e)}")
            return False
    
    def create_comment_view(self, parent):
        """Create the main comment grid view"""
        # Frame for treeview and scrollbar
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Create scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical")
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal")
        
        # Configure the treeview
        columns = ("idx", "comment", "label", "flag", "platform", "length")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="headings",
                                 yscrollcommand=vsb.set, xscrollcommand=hsb.set,
                                 selectmode="extended")
        
        # Set column widths and headings
        self.tree.heading("idx", text="Index")
        self.tree.heading("comment", text="Comment Text")
        self.tree.heading("label", text="Label")
        self.tree.heading("flag", text="Delete")
        self.tree.heading("platform", text="Platform")
        self.tree.heading("length", text="Words")
        
        self.tree.column("idx", width=60, stretch=False)
        self.tree.column("comment", width=800, stretch=True)
        self.tree.column("label", width=150, stretch=False)
        self.tree.column("flag", width=60, stretch=False)
        self.tree.column("platform", width=100, stretch=False)
        self.tree.column("length", width=60, stretch=False)
        
        # Set up scrollbars
        vsb.configure(command=self.tree.yview)
        hsb.configure(command=self.tree.xview)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")
        
        # Pack the treeview
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        # Bind selection event
        self.tree.bind("<<TreeviewSelect>>", self.on_comment_selected)
        
        # Right-click menu - Updated menu items
        self.context_menu = tk.Menu(self.tree, tearoff=0)
        self.context_menu.add_command(label="Set to PHẢN ĐỘNG (1)", command=lambda: self.batch_update_labels("PHAN_DONG"))
        self.context_menu.add_command(label="Set to KHÔNG PHẢN ĐỘNG (2)", command=lambda: self.batch_update_labels("KHONG_PHAN_DONG"))
        self.context_menu.add_command(label="Set to KHÔNG LIÊN QUAN (3)", command=lambda: self.batch_update_labels("KHONG_LIEN_QUAN"))
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Mark for Deletion (4)", command=self.mark_for_deletion)
        self.context_menu.add_command(label="Unmark (5)", command=self.unmark_deletion)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Edit Comment Text (F2)", command=lambda: self.show_full_comment(None))
        
        self.tree.bind("<Button-3>", self.show_context_menu)
        
        # Double-click to toggle delete flag
        self.tree.bind("<Double-1>", self.toggle_delete_flag)
        
        # Add this binding for viewing full comment text
        self.tree.bind("<F2>", self.show_full_comment)
        self.tree.bind("<Return>", self.show_full_comment)
    
        # Populate the treeview
        self.populate_tree()
    
    def populate_tree(self, filter_expr=None, search_text=None):
        """Populate the treeview with data"""
        # Clear existing items
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Apply filter if provided
        if filter_expr is None:
            filtered_df = self.df
        else:
            filtered_df = self.df[filter_expr]
        
        # Apply search if provided
        if search_text:
            search_text = search_text.lower()
            comment_col = 'comment_raw' if 'comment_raw' in filtered_df.columns else 'comment'
            if comment_col in filtered_df.columns:
                filtered_df = filtered_df[filtered_df[comment_col].apply(
                    lambda x: search_text in str(x).lower() if isinstance(x, str) else False
                )]
        
        # Add items to treeview
        for idx, row in filtered_df.iterrows():
            comment_col = 'comment_raw' if 'comment_raw' in self.df.columns else 'comment'
            comment = str(row[comment_col]) if comment_col in row and isinstance(row[comment_col], str) else "[NO TEXT]"
            
            # Label text - now using text labels
            label_text = str(row['label']) if 'label' in row else "KHONG_LIEN_QUAN"
            
            # Calculate word count
            word_count = len(comment.split()) if isinstance(comment, str) else 0
            
            # Flag for deletion
            flag = "✓" if row['delete_flag'] else ""
            
            # Platform
            platform = row.get('platform', 'Unknown')
            
            # Insert into treeview with appropriate tag
            self.tree.insert("", "end", values=(idx, comment, label_text, flag, platform, word_count), 
                            tags=('short' if word_count < 5 else 'normal', 'flagged' if row['delete_flag'] else ''))
        
        # Configure tag appearance
        self.tree.tag_configure('short', background='lightyellow')
        self.tree.tag_configure('flagged', background='lightpink')
    
    def apply_filter(self):
        """Apply filter to the dataset view"""
        filter_value = self.filter_var.get()
        
        if filter_value == "All":
            self.populate_tree()
        elif filter_value == "short":
            # Filter for short comments
            comment_col = 'comment_raw' if 'comment_raw' in self.df.columns else 'comment'
            if comment_col in self.df.columns:
                self.populate_tree(self.df[comment_col].apply(
                    lambda x: len(str(x).split()) < 5 if isinstance(x, str) else True
                ))
        else:
            # Filter by label
            self.populate_tree(self.df['label'] == filter_value)
        
        self.update_stats()
    
    def apply_search(self):
        """Apply search to the dataset view"""
        search_text = self.search_var.get()
        filter_value = self.filter_var.get()
        
        if filter_value == "All":
            filter_expr = None
        elif filter_value == "short":
            comment_col = 'comment_raw' if 'comment_raw' in self.df.columns else 'comment'
            if comment_col in self.df.columns:
                filter_expr = self.df[comment_col].apply(
                    lambda x: len(str(x).split()) < 5 if isinstance(x, str) else True
                )
            else:
                filter_expr = None
        else:
            filter_expr = (self.df['label'] == filter_value)
        
        self.populate_tree(filter_expr, search_text)
        self.update_stats()
    
    def on_comment_selected(self, event):
        """Show the related post and summary when a comment is selected"""
        selection = self.tree.selection()
        if not selection:
            return
        
        # Get the first selected item
        item = selection[0]
        idx = int(self.tree.item(item, "values")[0])
        
        # Get the related post
        post_col = 'post_raw' if 'post_raw' in self.df.columns else 'post'
        post = self.df.loc[idx, post_col] if post_col in self.df.columns else "[NO POST]"
        
        # Update post text
        self.post_text.delete(1.0, tk.END)
        self.post_text.insert(tk.END, str(post) if isinstance(post, str) else "[NO POST TEXT]")
        
        # Get the summary
        summary = self.df.loc[idx, 'summary'] if 'summary' in self.df.columns else "[NO SUMMARY]"
        
        # Update summary text
        self.summary_text.delete(1.0, tk.END)
        self.summary_text.insert(tk.END, str(summary) if isinstance(summary, str) else "[NO SUMMARY TEXT]")
    
    def batch_update_labels(self, label_value):
        """Update labels for all selected comments"""
        selection = self.tree.selection()
        if not selection:
            return
        
        for item in selection:
            idx = int(self.tree.item(item, "values")[0])
            self.df.at[idx, 'label'] = label_value
            
            # Update the treeview
            values = list(self.tree.item(item, "values"))
            values[2] = label_value
            self.tree.item(item, values=values)
        
        self.update_stats()
        messagebox.showinfo("Success", f"Updated {len(selection)} records to label {label_value}")
    
    def mark_for_deletion(self):
        """Mark selected comments for deletion"""
        selection = self.tree.selection()
        if not selection:
            return
        
        for item in selection:
            idx = int(self.tree.item(item, "values")[0])
            self.df.at[idx, 'delete_flag'] = True
            
            # Update the treeview
            values = list(self.tree.item(item, "values"))
            values[3] = "✓"
            self.tree.item(item, values=values, tags='flagged')
        
        self.update_stats()
    
    def unmark_deletion(self):
        """Unmark selected comments from deletion"""
        selection = self.tree.selection()
        if not selection:
            return
        
        for item in selection:
            idx = int(self.tree.item(item, "values")[0])
            self.df.at[idx, 'delete_flag'] = False
            
            # Update the treeview
            values = list(self.tree.item(item, "values"))
            values[3] = ""
            
            # Update tag - check if it's a short comment
            word_count = int(values[5]) if values[5] and str(values[5]).isdigit() else 0
            tag = 'short' if word_count < 5 else 'normal'
            self.tree.item(item, values=values, tags=tag)
        
        self.update_stats()
    
    def toggle_delete_flag(self, event):
        """Toggle delete flag on double-click"""
        item = self.tree.identify_row(event.y)
        if not item:
            return
            
        # Get current delete status
        values = self.tree.item(item, "values")
        idx = int(values[0])
        current_flag = self.df.at[idx, 'delete_flag']
        
        # Toggle flag
        self.df.at[idx, 'delete_flag'] = not current_flag
        
        # Update treeview
        values = list(values)
        values[3] = "✓" if not current_flag else ""
        
        # Update tag
        word_count = int(values[5]) if values[5] and str(values[5]).isdigit() else 0
        tag = 'flagged' if not current_flag else ('short' if word_count < 5 else 'normal')
        self.tree.item(item, values=values, tags=tag)
        
        self.update_stats()
    
    def show_context_menu(self, event):
        """Show context menu on right-click"""
        if self.tree.identify_row(event.y):
            self.context_menu.post(event.x_root, event.y_root)
    
    def update_stats(self):
        """Update statistics label"""
        total = len(self.df)
        visible = len(self.tree.get_children())
        
        # Label counts
        label_counts = self.df['label'].value_counts().to_dict()
        pd_count = label_counts.get("PHAN_DONG", 0)
        kpd_count = label_counts.get("KHONG_PHAN_DONG", 0)
        klq_count = label_counts.get("KHONG_LIEN_QUAN", 0)
        
        # Flagged count
        flagged = self.df['delete_flag'].sum()
        
        stats = f"Showing {visible}/{total} | PD:{pd_count} | KPD:{kpd_count} | KLQ:{klq_count} | Flagged:{flagged}"
        self.stats_var.set(stats)
    
    def load_checkpoint(self):
        """Load checkpoint if it exists"""
        try:
            if os.path.exists(self.checkpoint_path):
                with open(self.checkpoint_path, 'r') as f:
                    checkpoint = json.load(f)
                
                # Check if checkpoint is for the same file - compare strings
                if checkpoint.get("file_path") == str(self.file_path):
                    # Get the filter and position
                    filter_value = checkpoint.get("filter", "All")
                    search_text = checkpoint.get("search", "")
                    current_idx = checkpoint.get("current_index", 0)
                    
                    # Apply filter and search
                    self.filter_var.set(filter_value)
                    self.search_var.set(search_text)
                    self.apply_filter()
                    
                    # Select and scroll to the saved position
                    if self.tree.get_children():
                        target_idx = checkpoint.get("record_index")
                        target_item = None
                        
                        # Find the item in the tree with this index
                        for item in self.tree.get_children():
                            values = self.tree.item(item, "values")
                            if int(values[0]) == target_idx:
                                target_item = item
                                break
                        
                        if target_item:
                            self.tree.selection_set(target_item)
                            self.tree.see(target_item)
                            self.on_comment_selected(None)
                            messagebox.showinfo("Checkpoint Loaded", 
                                f"Resumed from your last session at record index {target_idx}")
                        else:
                            if 0 <= current_idx < len(self.tree.get_children()):
                                item = self.tree.get_children()[current_idx]
                                self.tree.selection_set(item)
                                self.tree.see(item)
                                self.on_comment_selected(None)
                                messagebox.showinfo("Checkpoint Loaded", 
                                    "Resumed from your last session")
        except json.JSONDecodeError:
            # Handle corrupted checkpoint file
            print("Corrupted checkpoint file detected. Starting fresh.")
            if os.path.exists(self.checkpoint_path):
                os.remove(self.checkpoint_path)  # Remove corrupted file
        except Exception as e:
            print(f"Error loading checkpoint: {str(e)}")
    
    def save_checkpoint(self):
        """Save the current position and filter settings"""
        try:
            filter_value = self.filter_var.get()
            search_text = self.search_var.get()
            
            selection = self.tree.selection()
            current_idx = 0
            record_idx = 0
            
            if selection:
                current_idx = self.tree.index(selection[0])
                record_idx = int(self.tree.item(selection[0], "values")[0])
            
            checkpoint = {
                "file_path": str(self.file_path),  # Convert Path to string
                "filter": filter_value,
                "search": search_text,
                "current_index": current_idx,
                "record_index": record_idx,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            with open(self.checkpoint_path, 'w') as f:
                json.dump(checkpoint, f)
            
            return True
        except Exception as e:
            print(f"Error saving checkpoint: {str(e)}")
            return False
    
    def auto_save_checkpoint(self):
        """Auto-save the checkpoint every 5 minutes"""
        self.save_checkpoint()
        self.root.after(300000, self.auto_save_checkpoint)
    
    def on_closing(self):
        """Handle window closing event"""
        if self.save_checkpoint():
            messagebox.showinfo("Checkpoint Saved", 
                "Your position has been saved.\nYou can resume from here next time.")
        self.root.destroy()
    
    def save_progress(self):
        """Save current progress to continue labeling later - KEEPS delete flags"""
        try:
            # Save current state back to original file - KEEP delete_flag
            save_df = self.df.copy()
            save_df.to_excel(self.file_path, index=False)
            
            # Save checkpoint
            self.save_checkpoint()
            
            messagebox.showinfo("Progress Saved", 
                              f"Current progress saved to: {self.file_path}\n"
                              f"Checkpoint saved - you can resume from here next time.\n"
                              f"Delete flags preserved: {self.df['delete_flag'].sum()} items marked")
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save file: {str(e)}")

    def export_final(self):
        """Export final cleaned dataset when completely done - REMOVES delete flags"""
        try:
            # Create final output file path using version-specific filename
            file_dir = os.path.dirname(self.file_path)
            final_filename = config.get_version_filename(self.version, "final_dataset")
            output_path = os.path.join(file_dir, final_filename)
            
            # Create a filtered copy without flagged records
            clean_df = self.df[self.df['delete_flag'] == False].copy()
            
            # Remove the delete_flag column for final export
            if 'delete_flag' in clean_df.columns:
                clean_df = clean_df.drop(columns=['delete_flag'])
            
            # Ensure we have the required columns in the right order
            required_columns = ['post_id', 'post_raw', 'summary', 'comment_id', 'comment_raw', 'created_date', 'platform', 'label']
            existing_columns = [col for col in required_columns if col in clean_df.columns]
            clean_df = clean_df[existing_columns]
            
            # Save to the output path
            clean_df.to_excel(output_path, index=False)
            
            # Save checkpoint
            self.save_checkpoint()
            
            messagebox.showinfo("Final Dataset Exported", 
                             f"Final cleaned dataset saved to: {output_path}\n"
                             f"({len(clean_df)} records - {self.df['delete_flag'].sum()} items removed)\n"
                             f"Columns: {', '.join(existing_columns)}\n\n"
                             f"Ready for data splitting process!")
        except Exception as e:
            messagebox.showerror("Export Error", f"Could not save file: {str(e)}")

    def show_full_comment(self, event):
        """Show the full comment text in a popup window when F2 or Enter is pressed"""
        selection = self.tree.selection()
        if not selection:
            return

        item = selection[0]
        values = self.tree.item(item, "values")
        idx = int(values[0])
        
        # Get the full comment text
        comment_col = 'comment_raw' if 'comment_raw' in self.df.columns else 'comment'
        comment = self.df.loc[idx, comment_col] if comment_col in self.df.columns else "[NO TEXT]"
        if not isinstance(comment, str):
            comment = "[NO TEXT]"
        
        # Create a toplevel window
        preview = tk.Toplevel(self.root)
        preview.title(f"Edit Comment Text (Index: {idx})")
        preview.geometry("800x500")

        # Add text widget with scrollbar - EDITABLE
        text_frame = ttk.Frame(preview)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create scrollbars
        ysb = ttk.Scrollbar(text_frame, orient="vertical")
        xsb = ttk.Scrollbar(text_frame, orient="horizontal")
        
        # Create text widget - EDITABLE
        text = tk.Text(text_frame, wrap=tk.WORD, yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        
        # Configure scrollbars
        ysb.config(command=text.yview)
        xsb.config(command=text.xview)
        
        # Pack scrollbars
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Pack text widget
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Insert comment text
        text.insert(tk.END, comment)
        
        # Add info label
        info_frame = ttk.Frame(preview)
        info_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        word_count = len(comment.split()) if isinstance(comment, str) else 0
        platform = self.df.loc[idx, 'platform'] if 'platform' in self.df.columns else "Unknown"
        
        info_text = f"Words: {word_count} | Platform: {platform}"
        ttk.Label(info_frame, text=info_text).pack(side=tk.LEFT)
        
        # Add save button
        button_frame = ttk.Frame(info_frame)
        button_frame.pack(side=tk.RIGHT)
        
        ttk.Button(button_frame, text="Save (Ctrl+S)", 
                   command=lambda: self.save_comment_text_simple(preview, idx, text)).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(preview, text="Close (Esc)", command=preview.destroy).pack(pady=10)
        
        preview.bind("<Control-s>", lambda e: self.save_comment_text_simple(preview, idx, text))
        preview.bind("<Escape>", lambda e: preview.destroy())
        
        text.focus_set()

    def save_comment_text_simple(self, preview_window, idx, text_widget):
        """Save the edited comment text and close window"""
        try:
            new_text = text_widget.get(1.0, tk.END).strip()
            
            # Update dataframe
            comment_col = 'comment_raw' if 'comment_raw' in self.df.columns else 'comment'
            if comment_col in self.df.columns:
                self.df.at[idx, comment_col] = new_text
            
            # Update treeview for the selected item
            for item in self.tree.get_children():
                values = self.tree.item(item, "values")
                if int(values[0]) == idx:
                    values = list(values)
                    values[1] = new_text if len(new_text) <= 100 else new_text[:100] + "..."
                    
                    word_count = len(new_text.split()) if new_text else 0
                    values[5] = word_count
                    
                    if self.df.at[idx, 'delete_flag']:
                        tag = 'flagged'
                    else:
                        tag = 'short' if word_count < 5 else 'normal'
                    
                    self.tree.item(item, values=values, tags=tag)
                    break
            
            self.update_stats()
            preview_window.destroy()
            
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save text: {str(e)}")

def main():
    # Parse command line arguments
    args = parse_args()
    
    # Get version or specific file
    if args.file:
        file_path = args.file
    else:
        # Get version
        if args.version:
            version = args.version
        else:
            version = input("Enter version (e.g., v1, v2): ").strip()
            if not version:
                print("Version is required!")
                return
            
        # Get file path for this version - Use version-specific filename mapping
        if args.input:
            # Use custom input filename
            file_path = config.get_path(version, "output", filename=args.input)
        else:
            # Use version-specific default filename
            file_path = config.get_version_file_path(version, "dataset_clean_check")
    
    print(f"Opening file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found!")
        
        # Try alternative filename for version compatibility
        if not args.input and not args.file:
            alt_file_path = config.get_version_file_path(version, "gemini_labeled")
            if os.path.exists(alt_file_path):
                print(f"Trying alternative file: {alt_file_path}")
                file_path = alt_file_path
            else:
                print("No valid dataset file found!")
                return
        else:
            return
    
    # Show the main application
    root = tk.Tk()
    app = FastCommentAuditor(root, file_path)
    root.mainloop()

if __name__ == "__main__":
    main()