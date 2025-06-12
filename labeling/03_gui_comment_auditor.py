import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import os
from datetime import datetime
import json
import os.path

class FastCommentAuditor:
    def __init__(self, root, file_path):
        self.root = root
        self.root.title("Fast Comment Auditor")
        self.root.geometry("1400x800")
        
        # Load dataset
        self.file_path = file_path
        self.checkpoint_path = os.path.join(os.path.dirname(file_path), "audit_checkpoint.json")
        self.load_dataset()
        
        # Set up main frame
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create filter controls
        filter_frame = ttk.Frame(main_frame)
        filter_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Filter options
        ttk.Label(filter_frame, text="Filter:").grid(row=0, column=0, padx=5)
        self.filter_var = tk.StringVar(value="All")
        ttk.Radiobutton(filter_frame, text="All", variable=self.filter_var, value="All").grid(row=0, column=1, padx=5)
        ttk.Radiobutton(filter_frame, text="PHAN_DONG (1)", variable=self.filter_var, value="1").grid(row=0, column=2, padx=5)
        ttk.Radiobutton(filter_frame, text="KHONG_PHAN_DONG (2)", variable=self.filter_var, value="2").grid(row=0, column=3, padx=5)
        ttk.Radiobutton(filter_frame, text="KHONG_LIEN_QUAN (3)", variable=self.filter_var, value="3").grid(row=0, column=4, padx=5)
        ttk.Radiobutton(filter_frame, text="Short (<5 words)", variable=self.filter_var, value="short").grid(row=0, column=5, padx=5)
        ttk.Button(filter_frame, text="Apply Filter", command=self.apply_filter).grid(row=0, column=6, padx=10)
        
        # Search box
        ttk.Label(filter_frame, text="Search:").grid(row=0, column=7, padx=(20, 5))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(filter_frame, textvariable=self.search_var, width=20)
        search_entry.grid(row=0, column=8, padx=5)
        ttk.Button(filter_frame, text="Search", command=self.apply_search).grid(row=0, column=9, padx=5)
        
        # Quick edit controls
        edit_frame = ttk.Frame(main_frame)
        edit_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(edit_frame, text="Set selected comments to:").grid(row=0, column=0, padx=5)
        ttk.Button(edit_frame, text="PHAN_DONG (1)", command=lambda: self.batch_update_labels(1)).grid(row=0, column=1, padx=5)
        ttk.Button(edit_frame, text="KHONG_PHAN_DONG (2)", command=lambda: self.batch_update_labels(2)).grid(row=0, column=2, padx=5)
        ttk.Button(edit_frame, text="KHONG_LIEN_QUAN (3)", command=lambda: self.batch_update_labels(3)).grid(row=0, column=3, padx=5)
        ttk.Button(edit_frame, text="Mark for Deletion (4)", command=self.mark_for_deletion).grid(row=0, column=4, padx=5)
        ttk.Button(edit_frame, text="Unmark (5)", command=self.unmark_deletion).grid(row=0, column=5, padx=5)

        # Export buttons
        ttk.Button(edit_frame, text="Save Progress", command=self.save_progress).grid(row=0, column=6, padx=(30, 5))
        ttk.Button(edit_frame, text="Export Final", command=self.export_final).grid(row=0, column=7, padx=(5, 20))

        # Statistics label
        self.stats_var = tk.StringVar()
        ttk.Label(edit_frame, textvariable=self.stats_var).grid(row=0, column=8, padx=(20, 5))
        
        # Create comment view with treeview
        self.create_comment_view(main_frame)
        
        # Show post details when a comment is selected
        post_frame = ttk.LabelFrame(main_frame, text="Related Post")
        post_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.post_text = tk.Text(post_frame, height=8, wrap=tk.WORD)
        self.post_text.pack(fill=tk.X, padx=5, pady=5)
        
        # Update stats
        self.update_stats()
        
        # Bind keyboard shortcuts
        self.root.bind("1", lambda event: self.batch_update_labels(1))
        self.root.bind("2", lambda event: self.batch_update_labels(2))
        self.root.bind("3", lambda event: self.batch_update_labels(3))
        self.root.bind("4", lambda event: self.mark_for_deletion())
        self.root.bind("5", lambda event: self.unmark_deletion())
        self.root.bind("d", lambda event: self.mark_for_deletion())  # Keep 'd' as alternative
        self.root.bind("<Delete>", lambda event: self.mark_for_deletion())
        self.root.bind("u", lambda event: self.unmark_deletion())  # Keep 'u' as alternative
        
        # Load checkpoint if it exists
        self.load_checkpoint()
        
        # Add autosave timer (save checkpoint every 5 minutes)
        self.root.after(300000, self.auto_save_checkpoint)
        
        # Bind window close event to save checkpoint
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def load_dataset(self):
        try:
            self.df = pd.read_excel(self.file_path)
            
            # Add a delete flag column if not present
            if 'delete_flag' not in self.df.columns:
                self.df['delete_flag'] = False
                
            # Convert label to numeric if not already
            self.df['label'] = pd.to_numeric(self.df['label'], errors='coerce').fillna(3)
            
        except Exception as e:
            messagebox.showerror("Error", f"Could not load file: {str(e)}")
            self.root.destroy()
        
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
        self.tree.column("label", width=100, stretch=False)
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
        
        # Right-click menu
        self.context_menu = tk.Menu(self.tree, tearoff=0)
        self.context_menu.add_command(label="Set to PHAN_DONG (1)", command=lambda: self.batch_update_labels(1))
        self.context_menu.add_command(label="Set to KHONG_PHAN_DONG (2)", command=lambda: self.batch_update_labels(2))
        self.context_menu.add_command(label="Set to KHONG_LIEN_QUAN (3)", command=lambda: self.batch_update_labels(3))
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Mark for Deletion (4)", command=self.mark_for_deletion)
        self.context_menu.add_command(label="Unmark (5)", command=self.unmark_deletion)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Edit Comment Text (F2)", command=lambda: self.show_full_comment(None))
        
        self.tree.bind("<Button-3>", self.show_context_menu)
        
        # Double-click to toggle delete flag
        self.tree.bind("<Double-1>", self.toggle_delete_flag)
        
        # Add this binding for viewing full comment text
        self.tree.bind("<F2>", self.show_full_comment)  # F2 key to view full comment
        self.tree.bind("<Return>", self.show_full_comment)  # Enter key to view full comment
    
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
            filtered_df = filtered_df[filtered_df['comment_raw'].apply(
                lambda x: search_text in str(x).lower() if isinstance(x, str) else False
            )]
        
        # Add items to treeview
        for idx, row in filtered_df.iterrows():
            comment = str(row['comment_raw']) if isinstance(row['comment_raw'], str) else "[NO TEXT]"
            label_map = {1: "PHAN_DONG", 2: "KHONG_PHAN_DONG", 3: "KHONG_LIEN_QUAN"}
            label_text = label_map.get(row['label'], str(row['label']))
            
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
            self.populate_tree(self.df['comment_raw'].apply(
                lambda x: len(str(x).split()) < 5 if isinstance(x, str) else True
            ))
        else:
            # Filter by label
            self.populate_tree(self.df['label'] == int(filter_value))
        
        self.update_stats()
    
    def apply_search(self):
        """Apply search to the dataset view"""
        search_text = self.search_var.get()
        filter_value = self.filter_var.get()
        
        if filter_value == "All":
            filter_expr = None
        elif filter_value == "short":
            filter_expr = self.df['comment_raw'].apply(
                lambda x: len(str(x).split()) < 5 if isinstance(x, str) else True
            )
        else:
            filter_expr = (self.df['label'] == int(filter_value))
        
        self.populate_tree(filter_expr, search_text)
        self.update_stats()
    
    def on_comment_selected(self, event):
        """Show the related post when a comment is selected"""
        selection = self.tree.selection()
        if not selection:
            return
        
        # Get the first selected item
        item = selection[0]
        idx = int(self.tree.item(item, "values")[0])
        
        # Get the related post
        post = self.df.loc[idx, 'post_raw']
        
        # Update post text
        self.post_text.delete(1.0, tk.END)
        self.post_text.insert(tk.END, str(post) if isinstance(post, str) else "[NO POST TEXT]")
    
    def batch_update_labels(self, label_value):
        """Update labels for all selected comments"""
        selection = self.tree.selection()
        if not selection:
            return
        
        for item in selection:
            idx = int(self.tree.item(item, "values")[0])
            self.df.at[idx, 'label'] = label_value
            
            # Update the treeview
            label_map = {1: "PHAN_DONG", 2: "KHONG_PHAN_DONG", 3: "KHONG_LIEN_QUAN"}
            values = list(self.tree.item(item, "values"))
            values[2] = label_map.get(label_value, str(label_value))
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
            word_count = int(values[5]) if values[5].isdigit() else 0
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
        word_count = int(values[5]) if values[5].isdigit() else 0
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
        pd_count = label_counts.get(1, 0)
        kpd_count = label_counts.get(2, 0)
        klq_count = label_counts.get(3, 0)
        
        # Flagged count
        flagged = self.df['delete_flag'].sum()
        
        stats = f"Showing {visible}/{total} | 1:{pd_count} | 2:{kpd_count} | 3:{klq_count} | Flagged:{flagged}"
        self.stats_var.set(stats)
    
    def load_checkpoint(self):
        """Load checkpoint if it exists"""
        try:
            if os.path.exists(self.checkpoint_path):
                with open(self.checkpoint_path, 'r') as f:
                    checkpoint = json.load(f)
                
                # Check if checkpoint is for the same file
                if checkpoint.get("file_path") == self.file_path:
                    # Get the filter and position
                    filter_value = checkpoint.get("filter", "All")
                    search_text = checkpoint.get("search", "")
                    current_idx = checkpoint.get("current_index", 0)
                    
                    # Apply filter and search
                    self.filter_var.set(filter_value)
                    self.search_var.set(search_text)
                    self.apply_filter()  # This will also apply the search if set
                    
                    # Select and scroll to the saved position
                    if self.tree.get_children():
                        # Find the tree item that contains the index
                        target_idx = checkpoint.get("record_index")
                        target_item = None
                        
                        # Find the item in the tree with this index
                        for item in self.tree.get_children():
                            values = self.tree.item(item, "values")
                            if int(values[0]) == target_idx:
                                target_item = item
                                break
                        
                        if target_item:
                            # Select and see the item
                            self.tree.selection_set(target_item)
                            self.tree.see(target_item)
                            self.on_comment_selected(None)  # Show the related post
                            messagebox.showinfo("Checkpoint Loaded", 
                                f"Resumed from your last session at record index {target_idx}")
                        else:
                            # If record index not found in current view, just position at the checkpoint index
                            if 0 <= current_idx < len(self.tree.get_children()):
                                item = self.tree.get_children()[current_idx]
                                self.tree.selection_set(item)
                                self.tree.see(item)
                                self.on_comment_selected(None)
                                messagebox.showinfo("Checkpoint Loaded", 
                                    "Resumed from your last session")
        except Exception as e:
            print(f"Error loading checkpoint: {str(e)}")
    
    def save_checkpoint(self):
        """Save the current position and filter settings"""
        try:
            # Get the current filter and search settings
            filter_value = self.filter_var.get()
            search_text = self.search_var.get()
            
            # Get the current tree position and record index
            selection = self.tree.selection()
            current_idx = 0
            record_idx = 0
            
            if selection:
                # Get the item index in the tree view
                current_idx = self.tree.index(selection[0])
                # Get the actual record index from the tree values
                record_idx = int(self.tree.item(selection[0], "values")[0])
            
            # Create the checkpoint data
            checkpoint = {
                "file_path": self.file_path,
                "filter": filter_value,
                "search": search_text,
                "current_index": current_idx,
                "record_index": record_idx,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Save to file
            with open(self.checkpoint_path, 'w') as f:
                json.dump(checkpoint, f)
            
            return True
        except Exception as e:
            print(f"Error saving checkpoint: {str(e)}")
            return False
    
    def auto_save_checkpoint(self):
        """Auto-save the checkpoint every 5 minutes"""
        self.save_checkpoint()
        # Schedule the next autosave
        self.root.after(300000, self.auto_save_checkpoint)
    
    def on_closing(self):
        """Handle window closing event"""
        if self.save_checkpoint():
            messagebox.showinfo("Checkpoint Saved", 
                "Your position has been saved.\nYou can resume from here next time.")
        self.root.destroy()
    
    def export_dataset(self):
        """Export the cleaned dataset and save checkpoint"""
        try:
            # Create output file path - same directory but with "_cleaned" suffix
            file_dir = os.path.dirname(self.file_path)
            file_name = os.path.basename(self.file_path)
            name_parts = os.path.splitext(file_name)
            output_path = os.path.join(file_dir, f"{name_parts[0]}_cleaned{name_parts[1]}")
            
            # Create a filtered copy without flagged records
            clean_df = self.df[self.df['delete_flag'] == False].copy()
            
            # Remove the delete_flag column
            if 'delete_flag' in clean_df.columns:
                clean_df = clean_df.drop(columns=['delete_flag'])
            
            # Save to the output path
            clean_df.to_excel(output_path, index=False)
            
            # Also save the full version with flags (for continued editing)
            self.df.to_excel(self.file_path, index=False)
            
            # Save checkpoint
            self.save_checkpoint()
            
            messagebox.showinfo("Export Complete", 
                               f"Clean dataset saved to: {output_path}\n"
                               f"({len(clean_df)} records, delete_flag column removed)\n\n"
                               f"Original file with flags updated at: {self.file_path}\n"
                               f"(for continued editing)\n\n"
                               f"Checkpoint saved - you can resume from here next time.")
        except Exception as e:
            messagebox.showerror("Export Error", f"Could not save file: {str(e)}")
    
    def show_full_comment(self, event):
        """Show the full comment text in a popup window when F2 or Enter is pressed"""
        selection = self.tree.selection()
        if not selection:
            return

        # Get selected item
        item = selection[0]
        values = self.tree.item(item, "values")
        idx = int(values[0])
        
        # Get the full comment text
        comment = self.df.loc[idx, 'comment_raw']
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
        
        # Add info label with just basic info
        info_frame = ttk.Frame(preview)
        info_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        # Info text - simplified
        word_count = len(comment.split()) if isinstance(comment, str) else 0
        platform = self.df.loc[idx, 'platform'] if 'platform' in self.df.columns else "Unknown"
        
        info_text = f"Words: {word_count} | Platform: {platform}"
        ttk.Label(info_frame, text=info_text).pack(side=tk.LEFT)
        
        # Add save button
        button_frame = ttk.Frame(info_frame)
        button_frame.pack(side=tk.RIGHT)
        
        # Save text button - changed text to show the actual shortcut
        ttk.Button(button_frame, text="Save (Ctrl+S)", 
                   command=lambda: self.save_comment_text_simple(preview, idx, text)).pack(side=tk.LEFT, padx=5)
        
        # Close button
        ttk.Button(preview, text="Close (Esc)", command=preview.destroy).pack(pady=10)
        
        # Changed: Use Ctrl+S to save instead of Enter, so Enter can be used for line breaks
        preview.bind("<Control-s>", lambda e: self.save_comment_text_simple(preview, idx, text))
        preview.bind("<Escape>", lambda e: preview.destroy())
        
        # Remove the Enter binding that was conflicting
        # preview.bind("<Return>", lambda e: self.save_comment_text_simple(preview, idx, text))
        
        # Set focus to text widget for immediate editing
        text.focus_set()
    
    def update_label_from_preview(self, preview_window, idx, label_value):
        """Update label from the preview window"""
        # Update dataframe
        self.df.at[idx, 'label'] = label_value
        
        # Update treeview for the selected item
        for item in self.tree.get_children():
            values = self.tree.item(item, "values")
            if int(values[0]) == idx:
                # Update the label in the treeview
                label_map = {1: "PHAN_DONG", 2: "KHONG_PHAN_DONG", 3: "KHONG_LIEN_QUAN"}
                values = list(values)
                values[2] = label_map.get(label_value, str(label_value))
                self.tree.item(item, values=values)
                break
        
        # Update stats
        self.update_stats()
        
        # Close the preview window
        preview_window.destroy()

    def save_progress(self):
        """Save current progress to continue labeling later"""
        try:
            # Simply save current state back to original file
            self.df.to_excel(self.file_path, index=False)
            
            # Save checkpoint
            self.save_checkpoint()
            
            messagebox.showinfo("Progress Saved", 
                              f"Current progress saved to: {self.file_path}\n"
                              f"Checkpoint saved - you can resume from here next time.")
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save file: {str(e)}")

    def export_final(self):
        """Export final cleaned dataset when completely done"""
        try:
            # Create final output file path
            file_dir = os.path.dirname(self.file_path)
            output_path = os.path.join(file_dir, "dataset_labeled.xlsx")
            
            # Create a filtered copy without flagged records
            clean_df = self.df[self.df['delete_flag'] == False].copy()
            
            # Remove the delete_flag column
            if 'delete_flag' in clean_df.columns:
                clean_df = clean_df.drop(columns=['delete_flag'])
            
            # Save to the output path
            clean_df.to_excel(output_path, index=False)
            
            # Save checkpoint
            self.save_checkpoint()
            
            messagebox.showinfo("Final Dataset Exported", 
                             f"Final cleaned dataset saved to: {output_path}\n"
                             f"({len(clean_df)} records, delete_flag column removed)\n\n"
                             f"Your labeling work is complete!")
        except Exception as e:
            messagebox.showerror("Export Error", f"Could not save file: {str(e)}")

    def save_comment_text(self, preview_window, idx, text_widget):
        """Save the edited comment text"""
        try:
            # Get the edited text
            new_text = text_widget.get(1.0, tk.END).strip()
            
            # Update dataframe
            self.df.at[idx, 'comment_raw'] = new_text
            
            # Update treeview for the selected item
            for item in self.tree.get_children():
                values = self.tree.item(item, "values")
                if int(values[0]) == idx:
                    # Update the comment text in the treeview
                    values = list(values)
                    values[1] = new_text if len(new_text) <= 100 else new_text[:100] + "..."
                    
                    # Update word count
                    word_count = len(new_text.split()) if new_text else 0
                    values[5] = word_count
                    
                    # Update tag based on new word count
                    tag = 'short' if word_count < 5 else 'normal'
                    if self.df.at[idx, 'delete_flag']:
                        tag = 'flagged'
                    
                    self.tree.item(item, values=values, tags=tag)
                    break
            
            # Update stats
            self.update_stats()
            
            # Show success message
            messagebox.showinfo("Text Saved", "Comment text has been updated successfully!")
            
            # Optionally close the preview window
            # preview_window.destroy()
            
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save text: {str(e)}")

    def save_comment_text_simple(self, preview_window, idx, text_widget):
        """Save the edited comment text and close window"""
        try:
            # Get the edited text
            new_text = text_widget.get(1.0, tk.END).strip()
            
            # Update dataframe
            self.df.at[idx, 'comment_raw'] = new_text
            
            # Update treeview for the selected item
            for item in self.tree.get_children():
                values = self.tree.item(item, "values")
                if int(values[0]) == idx:
                    # Update the comment text in the treeview
                    values = list(values)
                    values[1] = new_text if len(new_text) <= 100 else new_text[:100] + "..."
                    
                    # Update word count
                    word_count = len(new_text.split()) if new_text else 0
                    values[5] = word_count
                    
                    # Update tag based on new word count and delete flag
                    if self.df.at[idx, 'delete_flag']:
                        tag = 'flagged'
                    else:
                        tag = 'short' if word_count < 5 else 'normal'
                    
                    self.tree.item(item, values=values, tags=tag)
                    break
            
            # Update stats
            self.update_stats()
            
            # Close the preview window immediately
            preview_window.destroy()
            
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save text: {str(e)}")

# Run the application
if __name__ == "__main__":
    # Use dataset1.xlsx directly instead of file dialog
    # C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\output\dataset1_restored.xlsx
    file_path = r"C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\output\dataset_clean_check.xlsx"
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found!")
        exit(0)
    
    # Show the main application
    root = tk.Tk()
    app = FastCommentAuditor(root, file_path)
    root.mainloop()