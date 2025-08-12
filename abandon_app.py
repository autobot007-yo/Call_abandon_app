#!/usr/bin/env python3
"""
MICC Abandon Call Analyzer - Streamlined Streamlit App

Simple web app for analyzing abandon calls from MICC data.

Requirements:
    pip install streamlit pandas openpyxl xlsxwriter

Usage:
    streamlit run micc_analyzer_app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import io
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="MICC Abandon Call Analyzer",
    page_icon="📞",
    layout="wide"
)

class MICCAnalyzer:
    def __init__(self):
        self.acd_data = None
        self.cdr_data = None
        self.analysis_results = None
        self.summary_stats = None
        
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
        """Determine business day (6:00 AM to 5:59:59 AM next day)."""
        try:
            if isinstance(call_time, str):
                dt = pd.to_datetime(call_time)
            else:
                dt = call_time
            
            if dt.hour < 6:
                business_date = dt.date() - timedelta(days=1)
            else:
                business_date = dt.date()
                
            return business_date.strftime('%d-%b-%Y')
        except:
            return None
    
    def process_acd_data(self, uploaded_file):
        """Process ACD call details data."""
        try:
            # Read Excel file
            df = pd.read_excel(uploaded_file, sheet_name=0)
            
            # Clean column names
            df.columns = [col if col is not None else f'col_{i}' for i, col in enumerate(df.columns)]
            df.columns = [str(col).strip() for col in df.columns]
            
            # Filter valid phone numbers
            df = df.dropna(subset=['Phone'])
            df = df[df['Phone'] != '']
            
            # Process time and date fields
            df['wait_time_seconds'] = df['Wait Time at ACD'].apply(self.time_to_seconds)
            df['wait_time_formatted'] = df['wait_time_seconds'].apply(self.seconds_to_time_format)
            df['business_day'] = df['Call Time'].apply(self.determine_business_day)
            df['call_datetime'] = pd.to_datetime(df['Call Time'])
            
            # Identify abandon calls (HUNGUP with wait time > 27 seconds)
            df['is_abandon'] = (df['Answered/Hungup'] == 'HUNGUP') & (df['wait_time_seconds'] > 27)
            
            self.acd_data = df
            return True, f"✅ ACD data loaded: {len(df)} records, {df['is_abandon'].sum()} abandon calls"
            
        except Exception as e:
            return False, f"❌ Error loading ACD data: {str(e)}"
    
    def process_cdr_data(self, uploaded_file):
        """Process CDR (Call Detail Record) data."""
        try:
            # Read Excel file
            df = pd.read_excel(uploaded_file, sheet_name=0)
            
            # Clean column names
            df.columns = [col if col is not None else f'col_{i}' for i, col in enumerate(df.columns)]
            df.columns = [str(col).strip() for col in df.columns]
            
            # Filter valid phone numbers
            df = df.dropna(subset=['Phone'])
            df = df[df['Phone'] != '']
            
            # Process fields
            df['business_day'] = df['Call Time'].apply(self.determine_business_day)
            df['call_datetime'] = pd.to_datetime(df['Call Time'])
            df['is_outbound'] = df['Call Type'] == 'outbound.manual.dial'
            
            self.cdr_data = df
            return True, f"✅ CDR data loaded: {len(df)} records, {df['is_outbound'].sum()} outbound calls"
            
        except Exception as e:
            return False, f"❌ Error loading CDR data: {str(e)}"
    
    def analyze_abandon_calls(self):
        """Perform abandon call analysis with 24-hour window logic."""
        try:
            # Get abandon and outbound calls
            abandon_calls = self.acd_data[self.acd_data['is_abandon']].copy()
            outbound_calls = self.cdr_data[self.cdr_data['is_outbound']].copy()
            
            # Group abandon calls by phone number
            abandon_by_phone = abandon_calls.groupby('Phone')
            analysis_results = []
            
            for phone, phone_abandon_calls in abandon_by_phone:
                # Get first abandon call
                phone_abandon_calls = phone_abandon_calls.sort_values('call_datetime')
                first_abandon = phone_abandon_calls.iloc[0]
                
                # Define 24-hour window
                window_start = first_abandon['call_datetime']
                window_end = window_start + timedelta(hours=24)
                
                # Check for outbound calls in window
                phone_outbound = outbound_calls[outbound_calls['Phone'] == phone]
                outbound_in_window = phone_outbound[
                    (phone_outbound['call_datetime'] >= window_start) &
                    (phone_outbound['call_datetime'] <= window_end)
                ]
                
                # Check for answered calls in window
                phone_answered = self.acd_data[
                    (self.acd_data['Phone'] == phone) &
                    (self.acd_data['Answered/Hungup'] == 'ANSWERED')
                ]
                answered_in_window = phone_answered[
                    (phone_answered['call_datetime'] >= window_start) &
                    (phone_answered['call_datetime'] <= window_end)
                ]
                
                # Determine if truly abandoned
                is_abandoned = (len(outbound_in_window) == 0) and (len(answered_in_window) == 0)
                
                analysis_results.append({
                    'phone': phone,
                    'total_abandon_calls': len(phone_abandon_calls),
                    'first_abandon_time': first_abandon['Call Time'],
                    'first_abandon_business_day': first_abandon['business_day'],
                    'wait_time_seconds': first_abandon['wait_time_seconds'],
                    'wait_time_formatted': first_abandon['wait_time_formatted'],
                    'outbound_calls_in_window': len(outbound_in_window),
                    'answered_calls_in_window': len(answered_in_window),
                    'is_abandoned_number': is_abandoned,
                    'window_end': window_end
                })
            
            self.analysis_results = pd.DataFrame(analysis_results)
            
            # Calculate summary statistics
            total_inbound = len(self.acd_data)
            total_answered = len(self.acd_data[self.acd_data['Answered/Hungup'] == 'ANSWERED'])
            total_hungup = len(self.acd_data[self.acd_data['Answered/Hungup'] == 'HUNGUP'])
            total_abandon_calls = len(abandon_calls)
            unique_abandon_numbers = len(self.analysis_results)
            truly_abandoned = self.analysis_results['is_abandoned_number'].sum()
            contacted_within_24h = unique_abandon_numbers - truly_abandoned
            total_outbound = len(outbound_calls)
            
            self.summary_stats = {
                'total_inbound_calls': total_inbound,
                'total_answered_calls': total_answered,
                'total_hungup_calls': total_hungup,
                'total_abandon_calls': total_abandon_calls,
                'unique_abandon_numbers': unique_abandon_numbers,
                'truly_abandoned_numbers': truly_abandoned,
                'contacted_within_24h': contacted_within_24h,
                'total_outbound_calls': total_outbound,
                'answer_rate': round(total_answered / total_inbound * 100, 1) if total_inbound > 0 else 0,
                'hungup_rate': round(total_hungup / total_inbound * 100, 1) if total_inbound > 0 else 0,
                'abandon_rate': round(total_abandon_calls / total_inbound * 100, 1) if total_inbound > 0 else 0,
                'sla_abandon_rate': round(truly_abandoned / unique_abandon_numbers * 100, 1) if unique_abandon_numbers > 0 else 0
            }
            
            return True, f"✅ Analysis completed: {unique_abandon_numbers} unique numbers analyzed, {truly_abandoned} truly abandoned"
            
        except Exception as e:
            return False, f"❌ Analysis failed: {str(e)}"
    
    def generate_daily_breakdown(self):
        """Generate daily performance breakdown."""
        daily_stats = self.acd_data.groupby('business_day').agg({
            'Phone': 'count',
            'Answered/Hungup': [
                lambda x: (x == 'HUNGUP').sum(),
                lambda x: (x == 'ANSWERED').sum()
            ],
            'is_abandon': 'sum'
        })
        
        daily_stats.columns = ['total_calls', 'hungup_calls', 'answered_calls', 'abandon_calls']
        daily_stats = daily_stats.reset_index()
        
        # Add abandon analysis
        abandon_breakdown = self.analysis_results.groupby('first_abandon_business_day').agg({
            'phone': 'count',
            'is_abandoned_number': ['sum', lambda x: (1 - x).sum()]
        })
        abandon_breakdown.columns = ['unique_numbers', 'truly_abandoned', 'contacted']
        abandon_breakdown = abandon_breakdown.reset_index()
        abandon_breakdown.rename(columns={'first_abandon_business_day': 'business_day'}, inplace=True)
        
        # Merge and calculate rates
        breakdown = daily_stats.merge(abandon_breakdown, on='business_day', how='left').fillna(0)
        breakdown['answer_rate'] = (breakdown['answered_calls'] / breakdown['total_calls'] * 100).round(1)
        breakdown['hungup_rate'] = (breakdown['hungup_calls'] / breakdown['total_calls'] * 100).round(1)
        breakdown['abandon_rate'] = (breakdown['abandon_calls'] / breakdown['total_calls'] * 100).round(1)
        
        return breakdown
    
    def export_excel_report(self):
        """Generate Excel report and return as bytes."""
        try:
            output = io.BytesIO()
            
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                # Summary sheet
                summary_data = {
                    'Metric': [
                        'Total Inbound Calls', 'Total Answered Calls', 'Total Hungup Calls',
                        'Total Abandon Calls (>27s)', 'Unique Phone Numbers with Abandons',
                        'Truly Abandoned Numbers', 'Contacted Within 24h', 'Total Outbound Calls',
                        'Answer Rate (%)', 'Hungup Rate (%)', 'Abandon Rate (%)', 'SLA Abandon Rate (%)'
                    ],
                    'Value': [
                        self.summary_stats['total_inbound_calls'],
                        self.summary_stats['total_answered_calls'],
                        self.summary_stats['total_hungup_calls'],
                        self.summary_stats['total_abandon_calls'],
                        self.summary_stats['unique_abandon_numbers'],
                        self.summary_stats['truly_abandoned_numbers'],
                        self.summary_stats['contacted_within_24h'],
                        self.summary_stats['total_outbound_calls'],
                        self.summary_stats['answer_rate'],
                        self.summary_stats['hungup_rate'],
                        self.summary_stats['abandon_rate'],
                        self.summary_stats['sla_abandon_rate']
                    ]
                }
                
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                
                # Detailed analysis
                detailed_df = self.analysis_results.copy()
                detailed_df['status'] = detailed_df['is_abandoned_number'].map({True: 'Abandoned', False: 'Contacted'})
                detailed_df = detailed_df[[
                    'phone', 'first_abandon_business_day', 'first_abandon_time',
                    'wait_time_formatted', 'total_abandon_calls',
                    'outbound_calls_in_window', 'answered_calls_in_window', 'status'
                ]]
                detailed_df.columns = [
                    'Phone Number', 'Business Day', 'First Abandon Time',
                    'Wait Time (HH:MM:SS)', 'Total Abandon Calls',
                    'Outbound Calls in 24h', 'Answered Calls in 24h', 'Status'
                ]
                detailed_df.to_excel(writer, sheet_name='Detailed Analysis', index=False)
                
                # Daily performance
                daily_breakdown = self.generate_daily_breakdown()
                daily_breakdown.columns = [
                    'Business Day', 'Total Calls', 'Hungup Calls', 'Answered Calls',
                    'Abandon Calls', 'Unique Numbers', 'Truly Abandoned', 'Contacted',
                    'Answer Rate (%)', 'Hungup Rate (%)', 'Abandon Rate (%)'
                ]
                daily_breakdown.to_excel(writer, sheet_name='Daily Performance', index=False)
                
                # Callback list (abandoned numbers only)
                callback_list = self.analysis_results[
                    self.analysis_results['is_abandoned_number'] == True
                ].copy()
                if not callback_list.empty:
                    callback_df = callback_list[[
                        'phone', 'first_abandon_business_day', 'first_abandon_time', 'wait_time_formatted'
                    ]].copy()
                    callback_df.columns = ['Phone Number', 'Business Day', 'First Abandon Time', 'Wait Time']
                    callback_df.to_excel(writer, sheet_name='Callback List', index=False)
            
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            st.error(f"Error generating Excel report: {str(e)}")
            return None

def main():
    # Header
    st.title("📞 MICC Abandon Call Analyzer")
    st.markdown("Upload your ACD and CDR files to analyze abandon calls and generate comprehensive reports.")
    st.markdown("---")
    
    # Initialize session state
    if 'analyzer' not in st.session_state:
        st.session_state.analyzer = MICCAnalyzer()
        st.session_state.analysis_complete = False
    
    # File upload section
    st.header("📁 File Upload")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 ACD Data File")
        acd_file = st.file_uploader(
            "Upload ACD call details file",
            type=['xls', 'xlsx'],
            key="acd_file",
            help="Upload your ACD (Automatic Call Distribution) data file containing call details and wait times"
        )
        
        if acd_file:
            st.success(f"✅ ACD file uploaded: {acd_file.name}")
    
    with col2:
        st.subheader("📞 CDR Data File")
        cdr_file = st.file_uploader(
            "Upload CDR call records file",
            type=['xls', 'xlsx'],
            key="cdr_file",
            help="Upload your CDR (Call Detail Record) file containing outbound call information"
        )
        
        if cdr_file:
            st.success(f"✅ CDR file uploaded: {cdr_file.name}")
    
    st.markdown("---")
    
    # Analysis section
    st.header("🚀 Analysis")
    
    if acd_file and cdr_file:
        if st.button("📈 Analyze Data", type="primary", use_container_width=True):
            with st.spinner("Processing data and performing analysis..."):
                
                # Create progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Process ACD data
                status_text.text("Loading ACD data...")
                progress_bar.progress(20)
                acd_success, acd_message = st.session_state.analyzer.process_acd_data(acd_file)
                
                if not acd_success:
                    st.error(acd_message)
                    st.stop()
                
                # Process CDR data
                status_text.text("Loading CDR data...")
                progress_bar.progress(40)
                cdr_success, cdr_message = st.session_state.analyzer.process_cdr_data(cdr_file)
                
                if not cdr_success:
                    st.error(cdr_message)
                    st.stop()
                
                # Perform analysis
                status_text.text("Analyzing abandon calls...")
                progress_bar.progress(70)
                analysis_success, analysis_message = st.session_state.analyzer.analyze_abandon_calls()
                
                if not analysis_success:
                    st.error(analysis_message)
                    st.stop()
                
                # Complete
                progress_bar.progress(100)
                status_text.text("Analysis complete!")
                st.session_state.analysis_complete = True
                
                # Clean up progress indicators
                progress_bar.empty()
                status_text.empty()
                
                st.success("🎉 Analysis completed successfully!")
    else:
        st.info("👆 Please upload both ACD and CDR files to proceed with analysis.")
    
    # Results section
    if st.session_state.analysis_complete and st.session_state.analyzer.summary_stats:
        st.markdown("---")
        st.header("📊 Analysis Summary")
        
        stats = st.session_state.analyzer.summary_stats
        
        # Key metrics in columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="📞 Total Inbound Calls",
                value=f"{stats['total_inbound_calls']:,}"
            )
            st.metric(
                label="✅ Answered Calls",
                value=f"{stats['total_answered_calls']:,}",
                delta=f"{stats['answer_rate']}% Answer Rate"
            )
        
        with col2:
            st.metric(
                label="❌ Hungup Calls", 
                value=f"{stats['total_hungup_calls']:,}",
                delta=f"{stats['hungup_rate']}% Hungup Rate"
            )
            st.metric(
                label="⚠️ Abandon Calls (>27s)",
                value=f"{stats['total_abandon_calls']:,}",
                delta=f"{stats['abandon_rate']}% of Inbound"
            )
        
        with col3:
            st.metric(
                label="🔢 Unique Abandon Numbers",
                value=f"{stats['unique_abandon_numbers']:,}"
            )
            st.metric(
                label="🔥 Truly Abandoned",
                value=f"{stats['truly_abandoned_numbers']:,}",
                delta=f"{stats['sla_abandon_rate']}% SLA Rate"
            )
        
        with col4:
            st.metric(
                label="📱 Total Outbound Calls",
                value=f"{stats['total_outbound_calls']:,}"
            )
            st.metric(
                label="✅ Contacted in 24h",
                value=f"{stats['contacted_within_24h']:,}"
            )
        
        # Summary insights
        st.subheader("💡 Key Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if stats['sla_abandon_rate'] <= 3:
                st.success(f"🎯 **Excellent SLA Performance**: {stats['sla_abandon_rate']}% abandon rate (Target: ≤3%)")
            elif stats['sla_abandon_rate'] <= 5:
                st.warning(f"⚠️ **Acceptable SLA Performance**: {stats['sla_abandon_rate']}% abandon rate (Target: ≤3%)")
            else:
                st.error(f"🚨 **SLA Performance Issue**: {stats['sla_abandon_rate']}% abandon rate (Target: ≤3%)")
        
        with col2:
            if stats['answer_rate'] >= 80:
                st.success(f"📞 **Good Answer Rate**: {stats['answer_rate']}% of calls answered")
            elif stats['answer_rate'] >= 70:
                st.warning(f"📞 **Fair Answer Rate**: {stats['answer_rate']}% of calls answered")
            else:
                st.error(f"📞 **Low Answer Rate**: {stats['answer_rate']}% of calls answered")
        
        # Daily breakdown preview
        st.subheader("📅 Daily Performance Preview")
        daily_breakdown = st.session_state.analyzer.generate_daily_breakdown()
        
        if not daily_breakdown.empty:
            # Show top 5 days
            preview_df = daily_breakdown.head(5)
            preview_df.columns = [
                'Business Day', 'Total Calls', 'Hungup', 'Answered', 'Abandons',
                'Unique Numbers', 'Truly Abandoned', 'Contacted', 'Answer %', 'Hungup %', 'Abandon %'
            ]
            st.dataframe(preview_df, use_container_width=True)
            
            if len(daily_breakdown) > 5:
                st.caption(f"Showing 5 of {len(daily_breakdown)} business days. Download full report for complete data.")
        
        # Download section
        st.markdown("---")
        st.header("📥 Download Report")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.write("**Comprehensive Excel report includes:**")
            st.write("• 📊 Executive summary with all key metrics")
            st.write("• 📋 Detailed phone-by-phone analysis")
            st.write("• 📅 Daily performance breakdown")
            st.write("• 📞 Priority callback list")
        
        with col2:
            if st.button("📊 Generate Excel Report", type="secondary", use_container_width=True):
                with st.spinner("Generating Excel report..."):
                    excel_data = st.session_state.analyzer.export_excel_report()
                    
                    if excel_data:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"MICC_Abandon_Analysis_{timestamp}.xlsx"
                        
                        st.download_button(
                            label="⬇️ Download Excel Report",
                            data=excel_data,
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                        st.success("✅ Excel report ready for download!")
                    else:
                        st.error("❌ Failed to generate Excel report")

if __name__ == "__main__":
    main()