#!/usr/bin/env python3
"""
MICC Abandon Call Analyzer - Standalone Version

This script analyzes abandon calls from MICC data according to SLA requirements.

Requirements:
    pip install pandas openpyxl xlsxwriter tkinter

Usage Options:
    1. GUI File Picker:        python standalone_analyzer.py
    2. Command Line:           python standalone_analyzer.py --acd "path/to/acd.xls" --cdr "path/to/cdr.xls"
    3. Interactive Menu:       python standalone_analyzer.py --interactive
    4. Use Config Paths:       python standalone_analyzer.py --use-config
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path
import warnings
import argparse
warnings.filterwarnings('ignore')

# Try to import tkinter for GUI file picker
try:
    import tkinter as tk
    from tkinter import filedialog, messagebox
    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False
    print("⚠️  GUI not available. Install tkinter for file picker: pip install tkinter")

# 🔧 ============= CONFIGURATION - DEFAULT FILE PATHS =============
# These are used when --use-config option is selected

DEFAULT_ACD_FILE = r"C:\Users\akshay.kharat\OneDrive - Qinecsa Solutions\New folder\acd_1_8_.xls"
DEFAULT_CDR_FILE = r"C:\Users\akshay.kharat\OneDrive - Qinecsa Solutions\New folder\Call_details_record_1_8_.xls"
DEFAULT_OUTPUT_FOLDER = r"C:\Users\akshay.kharat\OneDrive - Qinecsa Solutions\New folder\Reports"

# 🔧 ========================================================================


class AbandonCallAnalyzer:
    def __init__(self, acd_file=None, cdr_file=None):
        """Initialize the analyzer with data files."""
        self.acd_file = acd_file
        self.cdr_file = cdr_file
        self.acd_data = None
        self.cdr_data = None
        self.abandon_calls = None
        self.outbound_calls = None
        self.analysis_results = None
        
    def time_to_seconds(self, time_str):
        """Convert time string (HH:MM:SS) to seconds."""
        if pd.isna(time_str) or time_str == '00:00:00' or not time_str:
            return 0
        try:
            parts = str(time_str).split(':')
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        except (ValueError, IndexError):
            return 0
    
    def seconds_to_time_format(self, seconds):
        """Convert seconds back to HH:MM:SS format."""
        if pd.isna(seconds) or seconds == 0:
            return "00:00:00"
        try:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = int(seconds % 60)
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        except:
            return "00:00:00"
    
    def determine_business_day(self, call_time):
        """
        Determine business day based on call time.
        Business day runs from 6:00 AM to 5:59:59 AM next day.
        Returns the date in DD-MMM-YYYY format for easy manager filtering.
        """
        try:
            if isinstance(call_time, str):
                # Handle different date formats properly
                # First try to parse as pandas datetime (more robust)
                dt = pd.to_datetime(call_time, dayfirst=True)  # Assume DD-MM-YYYY format
            else:
                dt = call_time
            
            # If time is before 6 AM, it belongs to previous day's business day
            if dt.hour < 6:
                business_date = dt.date() - timedelta(days=1)
            else:
                business_date = dt.date()
                
            # Return in DD-MMM-YYYY format (e.g., "08-Aug-2025")
            return business_date.strftime('%d-%b-%Y')
        except Exception as e:
            print(f"Warning: Could not parse date '{call_time}': {e}")
            return None
    
    def determine_shift_date(self, call_time):
        """
        Determine shift date based on call time (for internal processing).
        Shift runs from 6:00 AM to 5:59 AM next day.
        """
        try:
            if isinstance(call_time, str):
                # Handle different date formats properly
                dt = pd.to_datetime(call_time, dayfirst=True)  # Assume DD-MM-YYYY format
            else:
                dt = call_time
            
            # If time is before 6 AM, it belongs to previous day's shift
            if dt.hour < 6:
                shift_date = dt.date() - timedelta(days=1)
            else:
                shift_date = dt.date()
                
            return shift_date.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Warning: Could not parse date '{call_time}': {e}")
            return None
    
    def load_acd_data(self):
        """Load and process ACD call details data."""
        print(f"Loading ACD data from: {self.acd_file}")
        
        try:
            # Read the Excel file
            df = pd.read_excel(self.acd_file, sheet_name=0)
            
            # Clean column names (remove None values and extra spaces)
            df.columns = [col if col is not None else f'col_{i}' for i, col in enumerate(df.columns)]
            df.columns = [str(col).strip() for col in df.columns]
            
            # Filter out rows with no phone number
            df = df.dropna(subset=['Phone'])
            df = df[df['Phone'] != '']
            
            # Process the data
            df['wait_time_seconds'] = df['Wait Time at ACD'].apply(self.time_to_seconds)
            df['wait_time_formatted'] = df['wait_time_seconds'].apply(self.seconds_to_time_format)
            df['shift_date'] = df['Call Time'].apply(self.determine_shift_date)
            df['business_day'] = df['Call Time'].apply(self.determine_business_day)
            df['call_datetime'] = pd.to_datetime(df['Call Time'])
            
            # Identify abandon calls: HUNGUP with wait time > 27 seconds
            df['is_abandon'] = (df['Answered/Hungup'] == 'HUNGUP') & (df['wait_time_seconds'] > 27)
            
            self.acd_data = df
            print(f"✅ ACD data loaded: {len(df)} records")
            print(f"📞 Abandon calls found: {df['is_abandon'].sum()}")
            
        except Exception as e:
            print(f"❌ Error loading ACD data: {e}")
            sys.exit(1)
    
    def load_cdr_data(self):
        """Load and process CDR (Call Detail Record) data."""
        print(f"Loading CDR data from: {self.cdr_file}")
        
        try:
            # Read the Excel file
            df = pd.read_excel(self.cdr_file, sheet_name=0)
            
            # Clean column names
            df.columns = [col if col is not None else f'col_{i}' for i, col in enumerate(df.columns)]
            df.columns = [str(col).strip() for col in df.columns]
            
            # Filter out rows with no phone number
            df = df.dropna(subset=['Phone'])
            df = df[df['Phone'] != '']
            
            # Process the data
            df['shift_date'] = df['Call Time'].apply(self.determine_shift_date)
            df['business_day'] = df['Call Time'].apply(self.determine_business_day)
            df['call_datetime'] = pd.to_datetime(df['Call Time'])
            df['is_outbound'] = df['Call Type'] == 'outbound.manual.dial'
            
            self.cdr_data = df
            print(f"✅ CDR data loaded: {len(df)} records")
            print(f"📱 Outbound calls found: {df['is_outbound'].sum()}")
            
        except Exception as e:
            print(f"❌ Error loading CDR data: {e}")
            sys.exit(1)
    
    def analyze_abandon_calls(self):
        """Perform the main abandon call analysis with 24-hour window logic."""
        print("\n🔍 Performing abandon call analysis...")
        
        # Get abandon calls and outbound calls
        self.abandon_calls = self.acd_data[self.acd_data['is_abandon']].copy()
        self.outbound_calls = self.cdr_data[self.cdr_data['is_outbound']].copy()
        
        # Calculate additional metrics from ACD data
        total_inbound_calls = len(self.acd_data)
        total_hungup_calls = len(self.acd_data[self.acd_data['Answered/Hungup'] == 'HUNGUP'])
        total_answered_calls = len(self.acd_data[self.acd_data['Answered/Hungup'] == 'ANSWERED'])
        
        # Group abandon calls by phone number
        abandon_by_phone = self.abandon_calls.groupby('Phone')
        
        analysis_results = []
        
        for phone, phone_abandon_calls in abandon_by_phone:
            # Sort by call time to get the first abandon call
            phone_abandon_calls = phone_abandon_calls.sort_values('call_datetime')
            first_abandon_call = phone_abandon_calls.iloc[0]
            
            # Define 24-hour window from first abandon call
            window_start = first_abandon_call['call_datetime']
            window_end = window_start + timedelta(hours=24)
            
            # Check for outbound calls within the 24-hour window
            phone_outbound_calls = self.outbound_calls[self.outbound_calls['Phone'] == phone]
            outbound_in_window = phone_outbound_calls[
                (phone_outbound_calls['call_datetime'] >= window_start) &
                (phone_outbound_calls['call_datetime'] <= window_end)
            ]
            
            # Check for answered calls within the 24-hour window
            phone_answered_calls = self.acd_data[
                (self.acd_data['Phone'] == phone) &
                (self.acd_data['Answered/Hungup'] == 'ANSWERED')
            ]
            answered_in_window = phone_answered_calls[
                (phone_answered_calls['call_datetime'] >= window_start) &
                (phone_answered_calls['call_datetime'] <= window_end)
            ]
            
            # Determine if this is truly an abandoned number
            is_abandoned_number = (len(outbound_in_window) == 0) and (len(answered_in_window) == 0)
            
            # Compile results
            result = {
                'phone': phone,
                'total_abandon_calls': len(phone_abandon_calls),
                'first_abandon_time': first_abandon_call['Call Time'],
                'first_abandon_shift': first_abandon_call['shift_date'],
                'first_abandon_business_day': first_abandon_call['business_day'],
                'wait_time_seconds': first_abandon_call['wait_time_seconds'],
                'wait_time_formatted': first_abandon_call['wait_time_formatted'],
                'window_start': window_start,
                'window_end': window_end,
                'outbound_calls_in_window': len(outbound_in_window),
                'answered_calls_in_window': len(answered_in_window),
                'is_abandoned_number': is_abandoned_number,
                'outbound_call_times': list(outbound_in_window['Call Time'].values) if len(outbound_in_window) > 0 else [],
                'answered_call_times': list(answered_in_window['Call Time'].values) if len(answered_in_window) > 0 else []
            }
            
            analysis_results.append(result)
        
        self.analysis_results = pd.DataFrame(analysis_results)
        
        # Summary statistics
        total_unique_phones = len(self.analysis_results)
        truly_abandoned = self.analysis_results['is_abandoned_number'].sum()
        contacted_within_24h = total_unique_phones - truly_abandoned
        
        print(f"\n📊 === ANALYSIS SUMMARY ===")
        print(f"📞 Total inbound calls: {total_inbound_calls}")
        print(f"❌ Total hungup calls: {total_hungup_calls}")
        print(f"✅ Total answered calls: {total_answered_calls}")
        print(f"⚠️  Total abandon calls (>27s wait): {len(self.abandon_calls)}")
        print(f"🔢 Unique phone numbers with abandon calls: {total_unique_phones}")
        print(f"❌ Truly abandoned numbers (no contact within 24h): {truly_abandoned}")
        print(f"✅ Numbers contacted within 24h: {contacted_within_24h}")
        print(f"📱 Total outbound calls: {len(self.outbound_calls)}")
        print(f"📈 Overall abandon rate: {(truly_abandoned/total_unique_phones*100):.1f}%")
        print(f"📈 Hungup rate: {(total_hungup_calls/total_inbound_calls*100):.1f}%")
        
        return self.analysis_results
    
    def generate_shift_breakdown(self):
        """Generate daily business day breakdown with call volume metrics."""
        # Get daily call volume metrics from ACD data
        daily_stats = self.acd_data.groupby('business_day').agg({
            'Phone': 'count',  # Total inbound calls
            'Answered/Hungup': [
                lambda x: (x == 'HUNGUP').sum(),  # Total hungup calls
                lambda x: (x == 'ANSWERED').sum()  # Total answered calls
            ],
            'is_abandon': 'sum'  # Total abandon calls (>27s wait)
        }).round(2)
        
        # Flatten column names
        daily_stats.columns = ['total_inbound_calls', 'total_hungup_calls', 'total_answered_calls', 'total_abandon_calls']
        daily_stats = daily_stats.reset_index()
        
        # Get abandon analysis breakdown
        abandon_breakdown = self.analysis_results.groupby('first_abandon_business_day').agg({
            'phone': 'count',
            'is_abandoned_number': ['sum', lambda x: (1 - x).sum()]
        }).round(2)
        
        # Flatten column names for abandon analysis
        abandon_breakdown.columns = ['unique_abandon_numbers', 'truly_abandoned', 'contacted_within_24h']
        abandon_breakdown = abandon_breakdown.reset_index()
        abandon_breakdown.rename(columns={'first_abandon_business_day': 'business_day'}, inplace=True)
        
        # Merge daily stats with abandon analysis
        shift_breakdown = daily_stats.merge(abandon_breakdown, on='business_day', how='left')
        
        # Fill NaN values with 0 for days with no abandon calls
        shift_breakdown = shift_breakdown.fillna(0)
        
        # Calculate rates
        shift_breakdown['hungup_rate'] = (shift_breakdown['total_hungup_calls'] / shift_breakdown['total_inbound_calls'] * 100).round(1)
        shift_breakdown['abandon_rate'] = (shift_breakdown['total_abandon_calls'] / shift_breakdown['total_inbound_calls'] * 100).round(1)
        shift_breakdown['answer_rate'] = (shift_breakdown['total_answered_calls'] / shift_breakdown['total_inbound_calls'] * 100).round(1)
        
        # Sort by date properly
        shift_breakdown['sort_date'] = pd.to_datetime(shift_breakdown['business_day'], format='%d-%b-%Y')
        shift_breakdown = shift_breakdown.sort_values('sort_date').drop('sort_date', axis=1)
        
        return shift_breakdown
    
    def export_to_excel(self, output_file):
        """Export analysis results to Excel file."""
        print(f"\n📁 Exporting results to: {output_file}")
        
        try:
            with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
                # Summary sheet with enhanced metrics
                total_inbound = len(self.acd_data)
                total_hungup = len(self.acd_data[self.acd_data['Answered/Hungup'] == 'HUNGUP'])
                total_answered = len(self.acd_data[self.acd_data['Answered/Hungup'] == 'ANSWERED'])
                
                summary_data = {
                    'Metric': [
                        'Total Inbound Calls',
                        'Total Answered Calls',
                        'Total Hungup Calls',
                        'Total Abandon Calls (>27s)',
                        'Unique Phone Numbers with Abandons',
                        'Truly Abandoned Numbers',
                        'Contacted Within 24h',
                        'Total Outbound Calls',
                        'Answer Rate (%)',
                        'Hungup Rate (%)',
                        'Abandon Rate (% of Inbound)',
                        'SLA Abandon Rate (% of Abandon Numbers)'
                    ],
                    'Value': [
                        total_inbound,
                        total_answered,
                        total_hungup,
                        len(self.abandon_calls),
                        len(self.analysis_results),
                        self.analysis_results['is_abandoned_number'].sum(),
                        len(self.analysis_results) - self.analysis_results['is_abandoned_number'].sum(),
                        len(self.outbound_calls),
                        round(total_answered / total_inbound * 100, 1),
                        round(total_hungup / total_inbound * 100, 1),
                        round(len(self.abandon_calls) / total_inbound * 100, 1),
                        round(self.analysis_results['is_abandoned_number'].mean() * 100, 1)
                    ]
                }
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Detailed analysis
                export_df = self.analysis_results.copy()
                export_df['status'] = export_df['is_abandoned_number'].map({True: 'Abandoned', False: 'Contacted'})
                
                # Reorder columns for better readability
                column_order = [
                    'phone', 'first_abandon_business_day', 'first_abandon_time', 
                    'wait_time_formatted', 'total_abandon_calls', 
                    'outbound_calls_in_window', 'answered_calls_in_window', 
                    'status', 'window_end'
                ]
                export_df = export_df[column_order]
                
                # Rename columns for manager-friendly names
                export_df.columns = [
                    'Phone Number', 'Business Day', 'First Abandon Time', 
                    'Wait Time (HH:MM:SS)', 'Total Abandon Calls', 
                    'Outbound Calls in 24h', 'Answered Calls in 24h', 
                    'Status', '24h Window End Time'
                ]
                
                export_df.to_excel(writer, sheet_name='Detailed Analysis', index=False)
                
                # Daily Performance with enhanced metrics
                shift_breakdown = self.generate_shift_breakdown()
                shift_breakdown.columns = [
                    'Business Day', 'Total Inbound Calls', 'Total Hungup Calls', 'Total Answered Calls',
                    'Total Abandon Calls (>27s)', 'Unique Numbers with Abandons', 
                    'Truly Abandoned Numbers', 'Contacted Within 24h',
                    'Hungup Rate (%)', 'Abandon Rate (%)', 'Answer Rate (%)'
                ]
                shift_breakdown.to_excel(writer, sheet_name='Daily Performance', index=False)
                
                # Raw abandon calls with formatted columns
                abandon_export = self.abandon_calls[[
                    'Phone', 'business_day', 'Call Time', 'wait_time_formatted', 'wait_time_seconds', 
                    'Call ID', 'Queue Name', 'Username', 'Hangup Details'
                ]].copy()
                abandon_export.columns = [
                    'Phone Number', 'Business Day', 'Call Time', 'Wait Time (HH:MM:SS)', 'Wait Time (Seconds)',
                    'Call ID', 'Queue Name', 'Agent Name', 'Hangup Reason'
                ]
                abandon_export.to_excel(writer, sheet_name='All Abandon Calls', index=False)
                
                # Note: Outbound calls are used for analysis but not exported as separate sheet
                # They're only needed to check if numbers were contacted within 24h window
                
                # Add a call volume summary sheet
                volume_summary = self.acd_data.groupby('business_day').agg({
                    'Phone': 'count',
                    'Answered/Hungup': [
                        lambda x: (x == 'ANSWERED').sum(),
                        lambda x: (x == 'HUNGUP').sum()
                    ],
                    'is_abandon': 'sum',
                    'wait_time_seconds': ['mean', 'max']
                }).round(2)
                
                # Flatten columns for volume summary
                volume_summary.columns = [
                    'Total Calls', 'Answered Calls', 'Hungup Calls', 
                    'Abandon Calls (>27s)', 'Avg Wait Time (s)', 'Max Wait Time (s)'
                ]
                volume_summary = volume_summary.reset_index()
                volume_summary['Answer Rate (%)'] = (volume_summary['Answered Calls'] / volume_summary['Total Calls'] * 100).round(1)
                volume_summary['Hungup Rate (%)'] = (volume_summary['Hungup Calls'] / volume_summary['Total Calls'] * 100).round(1)
                
                # Rename columns for better presentation
                volume_summary.columns = [
                    'Business Day', 'Total Inbound Calls', 'Answered Calls', 'Hungup Calls',
                    'Abandon Calls (>27s)', 'Avg Wait Time (s)', 'Max Wait Time (s)',
                    'Answer Rate (%)', 'Hungup Rate (%)'
                ]
                volume_summary.to_excel(writer, sheet_name='Call Volume Summary', index=False)
                
                # Add a business day filter helper sheet
                business_days = sorted(self.analysis_results['first_abandon_business_day'].unique())
                filter_helper = pd.DataFrame({
                    'Available Business Days': business_days,
                    'Instructions': ['Use these exact values to filter data in other sheets'] + [''] * (len(business_days) - 1)
                })
                filter_helper.to_excel(writer, sheet_name='Business Day Filter', index=False)
            
            print(f"✅ Excel report exported successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Error exporting to Excel: {e}")
            return False
    
    def print_detailed_results(self, limit=10):
        """Print detailed results for top N records."""
        print(f"\n📋 === DETAILED RESULTS (Top {limit}) ===")
        
        for idx, row in self.analysis_results.head(limit).iterrows():
            status_emoji = "❌" if row['is_abandoned_number'] else "✅"
            print(f"\n{status_emoji} Phone: {row['phone']}")
            print(f"   📅 Business Day: {row['first_abandon_business_day']}")
            print(f"   📞 First abandon: {row['first_abandon_time']}")
            print(f"   ⏱️  Wait time: {row['wait_time_formatted']} ({row['wait_time_seconds']} seconds)")
            print(f"   🔢 Total abandon calls: {row['total_abandon_calls']}")
            print(f"   📱 Outbound calls in 24h: {row['outbound_calls_in_window']}")
            print(f"   ✅ Answered calls in 24h: {row['answered_calls_in_window']}")
            print(f"   📊 Status: {'ABANDONED' if row['is_abandoned_number'] else 'CONTACTED'}")
            
            if row['outbound_call_times']:
                print(f"   📞 Outbound call times: {row['outbound_call_times'][:3]}{'...' if len(row['outbound_call_times']) > 3 else ''}")
            if row['answered_call_times']:
                print(f"   ✅ Answered call times: {row['answered_call_times'][:3]}{'...' if len(row['answered_call_times']) > 3 else ''}")
    
    def get_callback_list(self):
        """Get list of numbers that need callbacks."""
        abandoned_numbers = self.analysis_results[
            self.analysis_results['is_abandoned_number'] == True
        ].copy()
        
        # Sort by first abandon time (oldest first - highest priority)
        abandoned_numbers = abandoned_numbers.sort_values('first_abandon_time')
        
        print(f"\n📞 === NUMBERS NEEDING CALLBACK ({len(abandoned_numbers)} total) ===")
        for idx, row in abandoned_numbers.head(15).iterrows():
            hours_since = (datetime.now() - row['window_start']).total_seconds() / 3600
            priority = "🔥 HIGH" if hours_since > 48 else "⚠️  MEDIUM" if hours_since > 24 else "📋 NORMAL"
            print(f"{priority} | {row['phone']} | {row['first_abandon_business_day']} | "
                  f"First abandon: {row['first_abandon_time']} | Wait: {row['wait_time_formatted']}")
        
        if len(abandoned_numbers) > 15:
            print(f"   ... and {len(abandoned_numbers) - 15} more numbers")
        
        return abandoned_numbers
    
    def run_full_analysis(self, output_file=None):
        """Run the complete analysis workflow."""
        print("🚀 Starting MICC Abandon Call Analysis...")
        print("=" * 60)
        
        # Load data
        self.load_acd_data()
        self.load_cdr_data()
        
        # Perform analysis
        self.analyze_abandon_calls()
        
        # Generate shift breakdown
        shift_breakdown = self.generate_shift_breakdown()
        print(f"\n📅 === BUSINESS DAY BREAKDOWN ===")
        for _, row in shift_breakdown.iterrows():
            rate_emoji = "🔴" if row['hungup_rate'] > 30 else "🟡" if row['hungup_rate'] > 20 else "🟢"
            print(f"{rate_emoji} {row['business_day']}:")
            print(f"   📞 Inbound: {int(row['total_inbound_calls'])}, ✅ Answered: {int(row['total_answered_calls'])}, ❌ Hungup: {int(row['total_hungup_calls'])}")
            print(f"   ⚠️  Abandons: {int(row['total_abandon_calls'])}, 🔢 Unique Numbers: {int(row['unique_abandon_numbers'])}")
            print(f"   📊 Rates: Answer {row['answer_rate']}%, Hungup {row['hungup_rate']}%, Abandon {row['abandon_rate']}%")
        
        # Print detailed results
        self.print_detailed_results()
        
        # Show callback list
        self.get_callback_list()
        
        # Export to Excel if output file specified
        if output_file:
            success = self.export_to_excel(output_file)
            if success:
                print(f"\n📋 Detailed Excel report includes:")
                print(f"   📊 Summary: Overall statistics including inbound/hungup/answered call counts")
                print(f"   📋 Detailed Analysis: Phone-by-phone breakdown with formatted wait times")
                print(f"   📅 Daily Performance: Business day metrics (inbound, hungup, answered, abandon rates)")
                print(f"   📈 Call Volume Summary: Daily call volume with answer/hungup rates and wait times")
                print(f"   📞 All Abandon Calls: Raw abandon call data with business days")
                print(f"   🔍 Business Day Filter: Helper sheet for filtering by specific days")
                print(f"\n💡 Manager Tip: Use 'Business Day' column to filter data (e.g., '01-Aug-2025')")
                print(f"   Business days run from 6:00 AM to 5:59:59 AM next day")
                print(f"\n📈 New Metrics Added:")
                print(f"   • Total inbound calls per day")
                print(f"   • Total hungup calls per day (all hungups, not just >27s)")
                print(f"   • Answer rate, Hungup rate, Abandon rate percentages")
                print(f"   • Average and maximum wait times per day")
                print(f"\n💡 Note: Outbound calls are analyzed for 24h window logic but not exported separately")
        
        return self.analysis_results


def select_files_gui():
    """Use GUI file picker to select ACD and CDR files."""
    if not GUI_AVAILABLE:
        print("❌ GUI not available. Please use command line options instead.")
        return None, None, None
    
    print("🖱️  Opening file picker dialogs...")
    
    # Create a root window (hidden)
    root = tk.Tk()
    root.withdraw()
    root.lift()
    root.attributes('-topmost', True)
    
    try:
        # Select ACD file
        messagebox.showinfo("Select ACD File", "Please select your ACD data file (usually contains 'acd' in the name)")
        acd_file = filedialog.askopenfilename(
            title="Select ACD Data File",
            filetypes=[
                ("Excel files", "*.xls *.xlsx"),
                ("All files", "*.*")
            ],
            initialdir=str(Path.home())
        )
        
        if not acd_file:
            print("❌ No ACD file selected.")
            return None, None, None
        
        # Select CDR file
        messagebox.showinfo("Select CDR File", "Please select your CDR data file (usually contains 'call detail' or 'cdr' in the name)")
        cdr_file = filedialog.askopenfilename(
            title="Select CDR Data File",
            filetypes=[
                ("Excel files", "*.xls *.xlsx"),
                ("All files", "*.*")
            ],
            initialdir=str(Path(acd_file).parent)  # Start in same folder as ACD file
        )
        
        if not cdr_file:
            print("❌ No CDR file selected.")
            return None, None, None
        
        # Select output folder
        messagebox.showinfo("Select Output Folder", "Please select where to save the analysis report")
        output_folder = filedialog.askdirectory(
            title="Select Output Folder for Reports",
            initialdir=str(Path(cdr_file).parent)
        )
        
        if not output_folder:
            # Use same folder as input files if no output folder selected
            output_folder = str(Path(acd_file).parent)
            print(f"📁 Using input file folder for output: {output_folder}")
        
        return acd_file, cdr_file, output_folder
        
    except Exception as e:
        print(f"❌ Error in file selection: {e}")
        return None, None, None
    finally:
        root.destroy()

def select_files_interactive():
    """Interactive command-line file selection."""
    print("🔍 Interactive File Selection")
    print("=" * 40)
    
    # ACD file selection
    print("\n📊 ACD File Selection:")
    print("1. Enter full path")
    print("2. Browse current directory")
    print("3. Use default path")
    
    choice = input("Choose option (1-3): ").strip()
    
    if choice == "1":
        acd_file = input("Enter full path to ACD file: ").strip().strip('"')
    elif choice == "2":
        acd_file = browse_current_directory("ACD", ["*acd*", "*ACD*"])
    elif choice == "3":
        acd_file = DEFAULT_ACD_FILE
    else:
        print("❌ Invalid choice. Using default.")
        acd_file = DEFAULT_ACD_FILE
    
    if not acd_file or not Path(acd_file).exists():
        print(f"❌ ACD file not found: {acd_file}")
        return None, None, None
    
    # CDR file selection
    print("\n📞 CDR File Selection:")
    print("1. Enter full path")
    print("2. Browse current directory")
    print("3. Use default path")
    
    choice = input("Choose option (1-3): ").strip()
    
    if choice == "1":
        cdr_file = input("Enter full path to CDR file: ").strip().strip('"')
    elif choice == "2":
        cdr_file = browse_current_directory("CDR", ["*call*detail*", "*cdr*", "*CDR*"])
    elif choice == "3":
        cdr_file = DEFAULT_CDR_FILE
    else:
        print("❌ Invalid choice. Using default.")
        cdr_file = DEFAULT_CDR_FILE
    
    if not cdr_file or not Path(cdr_file).exists():
        print(f"❌ CDR file not found: {cdr_file}")
        return None, None, None
    
    # Output folder
    print("\n📁 Output Folder:")
    print("1. Enter full path")
    print("2. Use same folder as input files")
    print("3. Use default output folder")
    
    choice = input("Choose option (1-3): ").strip()
    
    if choice == "1":
        output_folder = input("Enter output folder path: ").strip().strip('"')
    elif choice == "2":
        output_folder = str(Path(acd_file).parent)
    elif choice == "3":
        output_folder = DEFAULT_OUTPUT_FOLDER
    else:
        output_folder = str(Path(acd_file).parent)
    
    return acd_file, cdr_file, output_folder

def browse_current_directory(file_type, patterns):
    """Browse current directory for files matching patterns."""
    current_dir = Path('.')
    matching_files = []
    
    for pattern in patterns:
        matching_files.extend(current_dir.glob(f"**/{pattern}.xls*"))
    
    if not matching_files:
        print(f"❌ No {file_type} files found in current directory.")
        return None
    
    print(f"\n📁 Found {file_type} files:")
    for i, file in enumerate(matching_files, 1):
        print(f"{i}. {file}")
    
    try:
        choice = int(input(f"Select {file_type} file (1-{len(matching_files)}): "))
        if 1 <= choice <= len(matching_files):
            return str(matching_files[choice - 1])
        else:
            print("❌ Invalid choice.")
            return None
    except ValueError:
        print("❌ Invalid input.")
        return None

def parse_command_line():
    """Parse command line arguments for file selection."""
    parser = argparse.ArgumentParser(description='MICC Abandon Call Analyzer')
    parser.add_argument('--acd', help='Path to ACD data file')
    parser.add_argument('--cdr', help='Path to CDR data file')
    parser.add_argument('--output', help='Output folder path')
    parser.add_argument('--interactive', action='store_true', help='Use interactive file selection')
    parser.add_argument('--use-config', action='store_true', help='Use default configured paths')
    parser.add_argument('--gui', action='store_true', help='Use GUI file picker (default if no other option)')
    
    return parser.parse_args()


def main():
    """Main function to run the analyzer."""
    print("🎯 MICC Abandon Call Analyzer")
    print("=" * 40)
    
    # Parse command line arguments
    args = parse_command_line()
    
    acd_file = None
    cdr_file = None
    output_folder = None
    
    # Determine file selection method
    if args.acd and args.cdr:
        # Command line file paths provided
        print("📝 Using command line file paths...")
        acd_file = args.acd
        cdr_file = args.cdr
        output_folder = args.output or str(Path(acd_file).parent)
        
    elif args.use_config:
        # Use configured default paths
        print("⚙️  Using configured default paths...")
        acd_file = DEFAULT_ACD_FILE
        cdr_file = DEFAULT_CDR_FILE
        output_folder = args.output or DEFAULT_OUTPUT_FOLDER
        
    elif args.interactive:
        # Interactive file selection
        print("🔍 Starting interactive file selection...")
        acd_file, cdr_file, output_folder = select_files_interactive()
        
    elif args.gui or GUI_AVAILABLE:
        # GUI file picker (default if available)
        print("🖱️  Using GUI file picker...")
        acd_file, cdr_file, output_folder = select_files_gui()
        
    else:
        # Fallback to interactive if GUI not available
        print("💻 GUI not available, using interactive mode...")
        acd_file, cdr_file, output_folder = select_files_interactive()
    
    # Validate file selection
    if not acd_file or not cdr_file or not output_folder:
        print("❌ File selection cancelled or failed.")
        return
    
    print(f"\n📁 Selected Files:")
    print(f"   📊 ACD File: {acd_file}")
    print(f"   📞 CDR File: {cdr_file}")
    print(f"   💾 Output Folder: {output_folder}")
    
    # Validate that files exist
    if not Path(acd_file).exists():
        print(f"❌ ACD file not found: {acd_file}")
        return
    
    if not Path(cdr_file).exists():
        print(f"❌ CDR file not found: {cdr_file}")
        return
    
    print(f"✅ ACD file found!")
    print(f"✅ CDR file found!")
    
    # Create output folder if it doesn't exist
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    print(f"✅ Output folder ready!")
    
    # Confirm before proceeding
    proceed = input(f"\n❓ Proceed with analysis? (y/n): ").strip().lower()
    if proceed not in ['y', 'yes']:
        print("❌ Analysis cancelled.")
        return
    
    # Initialize and run analyzer
    analyzer = AbandonCallAnalyzer(acd_file, cdr_file)
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = Path(output_folder) / f'abandon_call_report_{timestamp}.xlsx'
    
    try:
        results = analyzer.run_full_analysis(str(output_file))
        print(f"\n🎉 Analysis complete! Processed {len(results)} unique phone numbers.")
        print(f"📁 Detailed report saved to: {output_file}")
        
        # Open folder option
        if sys.platform == "win32":
            open_folder = input(f"\n❓ Open output folder? (y/n): ").strip().lower()
            if open_folder in ['y', 'yes']:
                os.startfile(output_folder)
        
    except Exception as e:
        print(f"❌ An error occurred during analysis: {e}")
        print(f"   Please check your data files and try again.")
        print(f"   Error details: {str(e)}")


if __name__ == "__main__":
    main()

"""
🔧 FILE SELECTION METHODS:

=== 1. GUI FILE PICKER (Default) ===
python standalone_analyzer.py
python standalone_analyzer.py --gui

Opens file dialogs to select ACD file, CDR file, and output folder.

=== 2. COMMAND LINE ARGUMENTS ===
python standalone_analyzer.py --acd "C:\path\to\acd.xls" --cdr "C:\path\to\cdr.xls"
python standalone_analyzer.py --acd "acd.xls" --cdr "cdr.xls" --output "Reports"

Specify files directly in the command.

=== 3. INTERACTIVE MODE ===
python standalone_analyzer.py --interactive

Text-based menu to select files step by step.

=== 4. USE CONFIGURED PATHS ===
python standalone_analyzer.py --use-config

Uses the DEFAULT paths set at the top of the script.

=== 5. EXAMPLES ===

# GUI picker (easiest):
python standalone_analyzer.py

# Command line with specific files:
python standalone_analyzer.py --acd "C:\Data\acd_1_8_.xls" --cdr "C:\Data\Call_details_record_1_8_.xls"

# Interactive selection:
python standalone_analyzer.py --interactive

# Use defaults from script:
python standalone_analyzer.py --use-config

# GUI with custom output:
python standalone_analyzer.py --gui --output "C:\Reports\MICC"

💡 TIPS:
- GUI mode is most user-friendly
- Command line is best for automation/scripting
- Interactive mode works without GUI libraries
- Configure DEFAULT paths for quick --use-config access
- Put file paths in quotes if they contain spaces
- Script will create output folder if it doesn't exist

🔧 CONFIGURATION:
Change DEFAULT_ACD_FILE, DEFAULT_CDR_FILE, and DEFAULT_OUTPUT_FOLDER 
at the top of the script for your common file locations.
"""
