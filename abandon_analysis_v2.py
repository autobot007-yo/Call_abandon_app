import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

class AbandonRecoveryAnalyzer:
    def __init__(self, acd_file_path, call_details_file_path):
        """
        Initialize the analyzer with file paths for ACD and Call Details Record files
        """
        self.acd_file_path = acd_file_path
        self.call_details_file_path = call_details_file_path
        self.results = {}
        
        # Initialize all dataframes to avoid NameError
        self.acd_data = None
        self.call_data = None
        self.abandon_calls_df = None
        self.all_calls_df = None
        self.sla_recoveries_df = None
        self.non_sla_recoveries_df = None
        self.never_recovered_df = None
        self.approach_analysis_df = None
        self.occupancy_analysis_df = None
        self.summary_stats = None
        
    def normalize_phone(self, phone_str):
        """Normalize phone number to last 10 digits for consistent matching"""
        if pd.isna(phone_str) or phone_str == '':
            return 'Unknown'
        
        # Remove all non-digits
        clean_phone = re.sub(r'\D', '', str(phone_str))
        
        # Take last 10 digits
        if len(clean_phone) >= 10:
            return clean_phone[-10:]
        return clean_phone if clean_phone else 'Unknown'
    
    def time_to_seconds(self, time_str):
        """Convert HH:MM:SS to total seconds"""
        if pd.isna(time_str) or time_str == '':
            return 0
        
        try:
            parts = str(time_str).split(':')
            if len(parts) == 3:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2])
                return hours * 3600 + minutes * 60 + seconds
        except:
            pass
        return 0
    
    def seconds_to_time(self, seconds):
        """Convert seconds to HH:MM:SS format"""
        if pd.isna(seconds) or seconds == 0:
            return '00:00:00'
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def load_data(self):
        """Load and process both ACD and Call Details Record files"""
        try:
            print("Loading ACD data...")
            # Load ACD data
            self.acd_data = pd.read_excel(self.acd_file_path)
            
            print("Loading Call Details Record data...")
            # Load Call Details Record data
            self.call_data = pd.read_excel(self.call_details_file_path)
            
            print(f"ACD data loaded: {len(self.acd_data)} rows")
            print(f"Call Details data loaded: {len(self.call_data)} rows")
            
            # Validate data loaded successfully
            if self.acd_data is None or len(self.acd_data) == 0:
                raise ValueError("ACD data is empty or failed to load")
            if self.call_data is None or len(self.call_data) == 0:
                raise ValueError("Call Details data is empty or failed to load")
                
            return True
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
        
    def extract_abandon_calls(self):
        """Extract abandon calls from ACD data (wait time > 27 seconds)"""
        try:
            print("Extracting abandon calls...")
            
            if self.acd_data is None:
                raise ValueError("ACD data not loaded. Call load_data() first.")
            
            # Get column indices for ACD data
            acd_cols = {
                'phone': 3,  # Phone column
                'status': 15,  # Answered/Hungup column
                'call_time': 17,  # Call Time column
                'wait_time': 21,  # Wait Time at ACD column
                'user_disposition_code': 34  # User Disposition Code column
            }
            
            abandon_calls = []
            
            for idx, row in self.acd_data.iterrows():
                if idx == 0:  # Skip header
                    continue
                    
                try:
                    status = row.iloc[acd_cols['status']]
                    wait_time_str = row.iloc[acd_cols['wait_time']]
                    wait_time_seconds = self.time_to_seconds(wait_time_str)
                    
                    if status == "HUNGUP" and wait_time_seconds > 27:
                        phone = self.normalize_phone(row.iloc[acd_cols['phone']])
                        call_time = row.iloc[acd_cols['call_time']]
                        
                        if phone != 'Unknown' and not pd.isna(call_time):
                            abandon_calls.append({
                                'phone': phone,
                                'raw_phone': row.iloc[acd_cols['phone']],
                                'call_time': pd.to_datetime(call_time),
                                'call_time_str': str(call_time),
                                'wait_time_seconds': wait_time_seconds,
                                'wait_time_formatted': self.seconds_to_time(wait_time_seconds),
                                'acd_user_disposition_code': str(row.iloc[acd_cols['user_disposition_code']]) if not pd.isna(row.iloc[acd_cols['user_disposition_code']]) else ''
                            })
                except Exception as e:
                    continue
            
            self.abandon_calls_df = pd.DataFrame(abandon_calls)
            print(f"Found {len(self.abandon_calls_df)} abandon calls")
            return self.abandon_calls_df
            
        except Exception as e:
            print(f"❌ Error extracting abandon calls: {e}")
            return pd.DataFrame()
    
    def extract_all_calls(self):
        """Extract all calls (inbound and outbound) from Call Details Record"""
        try:
            print("Extracting all calls from Call Details Record...")
            
            if self.call_data is None:
                raise ValueError("Call Details data not loaded. Call load_data() first.")
            
            # Get column indices for Call Details Record
            call_cols = {
                'call_time': 2,
                'phone': 9,
                'call_type': 18,
                'system_disposition': 19,
                'agent_name': 33,
                'agent_id': 32,
                'ivr_time': 25,
                'user_setup_time': 37,
                'user_ringing_time': 38,
                'user_talk_time': 39,
                'acw_duration': 40,
                'cdr_disposition_code': 34,  # CDR Disposition Code column
                'cdr_disposition_class': 35  # CDR Disposition Class column
            }
            
            all_calls = []
            
            for idx, row in self.call_data.iterrows():
                if idx == 0:  # Skip header
                    continue
                    
                try:
                    call_type = row.iloc[call_cols['call_type']]
                    system_disp = row.iloc[call_cols['system_disposition']]
                    agent_name = row.iloc[call_cols['agent_name']]
                    call_time = row.iloc[call_cols['call_time']]
                    
                    # Only process connected calls with agents
                    if (call_type in ["inbound.call.dial", "outbound.manual.dial"] and 
                        system_disp == "CONNECTED" and 
                        not pd.isna(agent_name) and str(agent_name).strip() != ""):
                        
                        phone = self.normalize_phone(row.iloc[call_cols['phone']])
                        
                        if phone != 'Unknown' and not pd.isna(call_time):
                            # Calculate total occupancy time
                            ivr_time = self.time_to_seconds(row.iloc[call_cols['ivr_time']])
                            setup_time = self.time_to_seconds(row.iloc[call_cols['user_setup_time']])
                            ring_time = self.time_to_seconds(row.iloc[call_cols['user_ringing_time']])
                            talk_time = self.time_to_seconds(row.iloc[call_cols['user_talk_time']])
                            acw_time = self.time_to_seconds(row.iloc[call_cols['acw_duration']])
                            
                            total_occupancy = ivr_time + setup_time + ring_time + talk_time + acw_time
                            
                            call_datetime = pd.to_datetime(call_time)
                            end_time = call_datetime + timedelta(seconds=total_occupancy)
                            
                            all_calls.append({
                                'phone': phone,
                                'raw_phone': row.iloc[call_cols['phone']],
                                'call_type': 'INBOUND' if call_type == "inbound.call.dial" else 'OUTBOUND',
                                'agent_name': str(agent_name).strip(),
                                'agent_id': row.iloc[call_cols['agent_id']],
                                'call_time': call_datetime,
                                'call_time_str': str(call_time),
                                'end_time': end_time,
                                'total_occupancy_seconds': total_occupancy,
                                'talk_time_seconds': talk_time,
                                'ivr_time_seconds': ivr_time,
                                'setup_time_seconds': setup_time,
                                'ring_time_seconds': ring_time,
                                'acw_time_seconds': acw_time,
                                'system_disposition': system_disp,
                                'cdr_disposition_code': str(row.iloc[call_cols['cdr_disposition_code']]) if not pd.isna(row.iloc[call_cols['cdr_disposition_code']]) else '',
                                'cdr_disposition_class': str(row.iloc[call_cols['cdr_disposition_class']]) if not pd.isna(row.iloc[call_cols['cdr_disposition_class']]) else ''
                            })
                except Exception as e:
                    continue
            
            self.all_calls_df = pd.DataFrame(all_calls)
            print(f"Found {len(self.all_calls_df)} connected calls")
            return self.all_calls_df
            
        except Exception as e:
            print(f"❌ Error extracting connected calls: {e}")
            return pd.DataFrame()
    
    def analyze_recoveries(self):
        """Analyze recovery patterns using SLA rules"""
        try:
            print("Analyzing recovery patterns...")
            
            # Validate required data exists
            if self.abandon_calls_df is None or len(self.abandon_calls_df) == 0:
                raise ValueError("No abandon calls data. Call extract_abandon_calls() first.")
            if self.all_calls_df is None or len(self.all_calls_df) == 0:
                raise ValueError("No connected calls data. Call extract_all_calls() first.")
            
            # Group calls by phone number
            calls_by_phone = self.all_calls_df.groupby('phone')
            
            sla_recoveries = []
            non_sla_recoveries = []
            never_recovered = []
            
            for _, abandon in self.abandon_calls_df.iterrows():
                phone = abandon['phone']
                abandon_time = abandon['call_time']
                
                # Get all calls for this phone number
                phone_calls = self.all_calls_df[self.all_calls_df['phone'] == phone]
                
                if phone_calls.empty:
                    never_recovered.append(abandon.to_dict())
                    continue
                
                # SLA Rule 1 & 2: Check for recovery within 24 hours
                window_24_before = abandon_time - timedelta(hours=24)
                window_24_after = abandon_time + timedelta(hours=24)
                
                # Check for answered calls within 24-hour window
                sla_recovery = phone_calls[
                    (phone_calls['call_time'] >= window_24_before) & 
                    (phone_calls['call_time'] <= window_24_after)
                ]
                
                if not sla_recovery.empty:
                    # Found SLA recovery
                    recovery_call = sla_recovery.iloc[0]
                    time_diff = abs((abandon_time - recovery_call['call_time']).total_seconds() / 3600)
                    
                    recovery_info = abandon.to_dict()
                    recovery_info.update({
                        'recovery_type': 'SLA Recovery',
                        'recovery_time': recovery_call['call_time'],
                        'recovery_time_str': recovery_call['call_time_str'],
                        'recovery_agent': recovery_call['agent_name'],
                        'recovery_call_type': recovery_call['call_type'],
                        'time_to_recovery_hours': time_diff,
                        'recovery_disposition': recovery_call['system_disposition'],
                        'recovery_cdr_disposition_code': recovery_call['cdr_disposition_code'],
                        'recovery_cdr_disposition_class': recovery_call['cdr_disposition_class']
                    })
                    sla_recoveries.append(recovery_info)
                    continue
                
                # Check for non-SLA recovery (beyond 24 hours)
                non_sla_recovery = phone_calls[phone_calls['call_time'] > window_24_after]
                
                if not non_sla_recovery.empty:
                    # Found non-SLA recovery
                    recovery_call = non_sla_recovery.iloc[0]
                    time_diff = (recovery_call['call_time'] - abandon_time).total_seconds() / 3600
                    days = int(time_diff // 24)
                    hours = int(time_diff % 24)
                    
                    recovery_info = abandon.to_dict()
                    recovery_info.update({
                        'recovery_type': 'Non-SLA Recovery',
                        'recovery_time': recovery_call['call_time'],
                        'recovery_time_str': recovery_call['call_time_str'],
                        'recovery_agent': recovery_call['agent_name'],
                        'recovery_call_type': recovery_call['call_type'],
                        'time_to_recovery_hours': time_diff,
                        'recovery_days': days,
                        'recovery_hours': hours,
                        'recovery_disposition': recovery_call['system_disposition'],
                        'recovery_cdr_disposition_code': recovery_call['cdr_disposition_code'],
                        'recovery_cdr_disposition_class': recovery_call['cdr_disposition_class']
                    })
                    non_sla_recoveries.append(recovery_info)
                    continue
                
                # Never recovered
                never_recovered.append(abandon.to_dict())
            
            self.sla_recoveries_df = pd.DataFrame(sla_recoveries)
            self.non_sla_recoveries_df = pd.DataFrame(non_sla_recoveries)
            self.never_recovered_df = pd.DataFrame(never_recovered)
            
            print(f"SLA recoveries: {len(self.sla_recoveries_df)}")
            print(f"Non-SLA recoveries: {len(self.non_sla_recoveries_df)}")
            print(f"Never recovered: {len(self.never_recovered_df)}")
            
            return self.sla_recoveries_df, self.non_sla_recoveries_df, self.never_recovered_df
            
        except Exception as e:
            print(f"❌ Error analyzing recoveries: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    def analyze_customer_approaches(self):
        """Analyze how many approaches each customer made before successful connection"""
        try:
            print("Analyzing customer approach patterns...")
            
            approach_analysis = []
            
            # Combine all recoveries
            all_recoveries = []
            if not self.sla_recoveries_df.empty:
                all_recoveries.extend(self.sla_recoveries_df.to_dict('records'))
            if not self.non_sla_recoveries_df.empty:
                all_recoveries.extend(self.non_sla_recoveries_df.to_dict('records'))
            
            for recovery in all_recoveries:
                phone = recovery['phone']
                recovery_time = recovery['recovery_time']
                
                # Count all calls (attempts) for this phone before successful recovery
                phone_calls = self.all_calls_df[self.all_calls_df['phone'] == phone]
                phone_abandons = self.abandon_calls_df[self.abandon_calls_df['phone'] == phone]
                
                # Count attempts before recovery
                attempts_before_recovery = phone_calls[phone_calls['call_time'] <= recovery_time]
                abandons_before_recovery = phone_abandons[phone_abandons['call_time'] <= recovery_time]
                
                total_attempts = len(attempts_before_recovery) + len(abandons_before_recovery)
                successful_calls = len(attempts_before_recovery)
                abandoned_calls = len(abandons_before_recovery)
                
                approach_info = {
                    'phone': phone,
                    'raw_phone': recovery['raw_phone'],
                    'recovery_time': recovery_time,
                    'recovery_agent': recovery['recovery_agent'],
                    'recovery_type': recovery['recovery_type'],
                    'total_attempts': total_attempts,
                    'successful_calls_before_recovery': successful_calls,
                    'abandoned_calls_before_recovery': abandoned_calls,
                    'final_recovery_call_type': recovery['recovery_call_type'],
                    'time_to_recovery_hours': recovery.get('time_to_recovery_hours', 0),
                    'customer_persistence_score': total_attempts,
                    'final_cdr_disposition_code': recovery.get('recovery_cdr_disposition_code', ''),
                    'final_cdr_disposition_class': recovery.get('recovery_cdr_disposition_class', '')
                }
                approach_analysis.append(approach_info)
            
            self.approach_analysis_df = pd.DataFrame(approach_analysis)
            print(f"Analyzed approach patterns for {len(self.approach_analysis_df)} recovered customers")
            
            return self.approach_analysis_df
            
        except Exception as e:
            print(f"❌ Error analyzing customer approaches: {e}")
            return pd.DataFrame()
    
    def analyze_agent_occupancy(self):
        """Analyze agent occupancy during abandon calls with 1-hour window"""
        try:
            print("Analyzing agent occupancy during abandon calls...")
            
            occupancy_analysis = []
            
            for _, abandon in self.abandon_calls_df.iterrows():
                abandon_time = abandon['call_time']
                
                # Create 1-hour window (±30 minutes)
                window_start = abandon_time - timedelta(minutes=30)
                window_end = abandon_time + timedelta(minutes=30)
                
                # Find agents busy during exact abandon time
                exactly_busy = self.all_calls_df[
                    (self.all_calls_df['call_time'] <= abandon_time) & 
                    (self.all_calls_df['end_time'] >= abandon_time)
                ]
                
                # Find agents busy during 1-hour window
                window_busy = self.all_calls_df[
                    (self.all_calls_df['call_time'] <= window_end) & 
                    (self.all_calls_df['end_time'] >= window_start)
                ]
                
                # Prepare busy agents details
                busy_agents_details = []
                for _, busy_call in exactly_busy.iterrows():
                    busy_agents_details.append({
                        'agent_name': busy_call['agent_name'],
                        'call_type': busy_call['call_type'],
                        'customer_phone': busy_call['phone'],
                        'call_start': busy_call['call_time_str'],
                        'call_end': busy_call['end_time'].strftime('%Y-%m-%d %H:%M:%S'),
                        'talk_duration': self.seconds_to_time(busy_call['talk_time_seconds']),
                        'total_occupancy': self.seconds_to_time(busy_call['total_occupancy_seconds'])
                    })
                
                occupancy_info = {
                    'abandon_phone': abandon['phone'],
                    'abandon_raw_phone': abandon['raw_phone'],
                    'abandon_time': abandon['call_time_str'],
                    'abandon_wait_time': abandon['wait_time_formatted'],
                    'window_start': window_start.strftime('%Y-%m-%d %H:%M:%S'),
                    'window_end': window_end.strftime('%Y-%m-%d %H:%M:%S'),
                    'agents_exactly_busy': len(exactly_busy),
                    'agents_in_window': len(window_busy),
                    'busy_agents_details': str(busy_agents_details) if busy_agents_details else 'No agents busy',
                    'capacity_issue': 'Yes' if len(exactly_busy) > 0 else 'No',
                    'system_issue': 'Yes' if len(exactly_busy) == 0 else 'No'
                }
                occupancy_analysis.append(occupancy_info)
            
            self.occupancy_analysis_df = pd.DataFrame(occupancy_analysis)
            print(f"Analyzed occupancy for {len(self.occupancy_analysis_df)} abandon calls")
            
            return self.occupancy_analysis_df
            
        except Exception as e:
            print(f"❌ Error analyzing agent occupancy: {e}")
            return pd.DataFrame()
    
    def generate_summary_statistics(self):
        """Generate overall summary statistics"""
        try:
            print("Generating summary statistics...")
            
            total_abandons = len(self.abandon_calls_df) if self.abandon_calls_df is not None else 0
            sla_recoveries = len(self.sla_recoveries_df) if self.sla_recoveries_df is not None else 0
            non_sla_recoveries = len(self.non_sla_recoveries_df) if self.non_sla_recoveries_df is not None else 0
            never_recovered = len(self.never_recovered_df) if self.never_recovered_df is not None else 0
            total_recoveries = sla_recoveries + non_sla_recoveries
            
            # Agent impact analysis
            agent_impact = {}
            if self.occupancy_analysis_df is not None:
                for _, row in self.occupancy_analysis_df.iterrows():
                    if row['agents_exactly_busy'] > 0:
                        # Parse busy agents details
                        try:
                            import ast
                            busy_details = ast.literal_eval(row['busy_agents_details'])
                            for agent_detail in busy_details:
                                agent_name = agent_detail['agent_name']
                                if agent_name not in agent_impact:
                                    agent_impact[agent_name] = 0
                                agent_impact[agent_name] += 1
                        except:
                            pass
            
            total_calls = len(self.all_calls_df) if self.all_calls_df is not None else 0
            capacity_issues = len(self.occupancy_analysis_df[self.occupancy_analysis_df['capacity_issue'] == 'Yes']) if self.occupancy_analysis_df is not None else 0
            system_issues = len(self.occupancy_analysis_df[self.occupancy_analysis_df['system_issue'] == 'Yes']) if self.occupancy_analysis_df is not None else 0
            
            summary_stats = {
                'Analysis Period': 'August 1-8, 2025',
                'Total Calls Processed': total_calls,
                'Total Abandon Calls': total_abandons,
                'SLA Recoveries (24hrs)': sla_recoveries,
                'Non-SLA Recoveries (>24hrs)': non_sla_recoveries,
                'Total Recovery Rate': f"{(total_recoveries/total_abandons*100):.1f}%" if total_abandons > 0 else "0.0%",
                'Never Recovered': never_recovered,
                'True Loss Rate': f"{(never_recovered/total_abandons*100):.1f}%" if total_abandons > 0 else "0.0%",
                'Capacity Issues': capacity_issues,
                'System Issues': system_issues,
                'Top Agent Impact': max(agent_impact.items(), key=lambda x: x[1]) if agent_impact else ('None', 0)
            }
            
            self.summary_stats = summary_stats
            return summary_stats
            
        except Exception as e:
            print(f"❌ Error generating summary statistics: {e}")
            return {}
    
    def save_to_excel(self, output_filename='abandon_recovery_analysis.xlsx'):
        """Save all analysis results to Excel file with multiple sheets"""
        try:
            print(f"Saving results to {output_filename}...")
            
            with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
                # Sheet 1: Summary Statistics
                if self.summary_stats:
                    summary_df = pd.DataFrame(list(self.summary_stats.items()), 
                                            columns=['Metric', 'Value'])
                    summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Sheet 2: Overall Abandon Calls
                if self.abandon_calls_df is not None and not self.abandon_calls_df.empty:
                    abandon_details = self.abandon_calls_df.copy()
                    abandon_details.to_excel(writer, sheet_name='Abandon_Calls', index=False)
                
                # Sheet 3: SLA Recoveries
                if self.sla_recoveries_df is not None and not self.sla_recoveries_df.empty:
                    sla_output = self.sla_recoveries_df.copy()
                    sla_output['time_to_recovery_formatted'] = sla_output['time_to_recovery_hours'].apply(
                        lambda x: f"{x:.1f} hours" if not pd.isna(x) else ""
                    )
                    sla_output.to_excel(writer, sheet_name='SLA_Recoveries', index=False)
                
                # Sheet 4: Non-SLA Recoveries
                if self.non_sla_recoveries_df is not None and not self.non_sla_recoveries_df.empty:
                    non_sla_output = self.non_sla_recoveries_df.copy()
                    non_sla_output['time_to_recovery_formatted'] = non_sla_output.apply(
                        lambda row: f"{row.get('recovery_days', 0)}d {row.get('recovery_hours', 0)}h" 
                        if not pd.isna(row.get('recovery_days', 0)) else "", axis=1
                    )
                    non_sla_output.to_excel(writer, sheet_name='Non_SLA_Recoveries', index=False)
                
                # Sheet 5: Never Recovered
                if self.never_recovered_df is not None and not self.never_recovered_df.empty:
                    self.never_recovered_df.to_excel(writer, sheet_name='Never_Recovered', index=False)
                
                # Sheet 6: Customer Approach Analysis
                if self.approach_analysis_df is not None and not self.approach_analysis_df.empty:
                    approach_output = self.approach_analysis_df.copy()
                    approach_output['time_to_recovery_formatted'] = approach_output['time_to_recovery_hours'].apply(
                        lambda x: f"{x:.1f} hours" if not pd.isna(x) else ""
                    )
                    approach_output.to_excel(writer, sheet_name='Customer_Approaches', index=False)
                
                # Sheet 7: Agent Occupancy Analysis
                if self.occupancy_analysis_df is not None and not self.occupancy_analysis_df.empty:
                    self.occupancy_analysis_df.to_excel(writer, sheet_name='Agent_Occupancy', index=False)
                
                # Sheet 8: All Connected Calls (for reference)
                if self.all_calls_df is not None and not self.all_calls_df.empty:
                    calls_output = self.all_calls_df.copy()
                    calls_output['total_occupancy_formatted'] = calls_output['total_occupancy_seconds'].apply(self.seconds_to_time)
                    calls_output['talk_time_formatted'] = calls_output['talk_time_seconds'].apply(self.seconds_to_time)
                    calls_output.to_excel(writer, sheet_name='All_Connected_Calls', index=False)
            
            print(f"Analysis complete! Results saved to {output_filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving to Excel: {e}")
            return False
    
    def run_complete_analysis(self, output_filename='abandon_recovery_analysis.xlsx'):
        """Run the complete analysis pipeline with proper error handling"""
        print("=" * 60)
        print("ABANDON & RECOVERY ANALYSIS - COMPREHENSIVE REPORT")
        print("=" * 60)
        
        try:
            # Step 1: Load data
            if not self.load_data():
                return None
            
            # Step 2: Extract abandon calls
            self.extract_abandon_calls()
            if self.abandon_calls_df is None or len(self.abandon_calls_df) == 0:
                print("❌ No abandon calls found. Analysis cannot continue.")
                return None
            
            # Step 3: Extract all calls - CRITICAL FIX
            self.extract_all_calls()
            if self.all_calls_df is None or len(self.all_calls_df) == 0:
                print("❌ No connected calls found. Analysis cannot continue.")
                return None
            
            # Step 4: Analyze recoveries - Now all_calls_df exists
            self.analyze_recoveries()
            
            # Step 5: Analyze customer approaches
            self.analyze_customer_approaches()
            
            # Step 6: Analyze agent occupancy
            self.analyze_agent_occupancy()
            
            # Step 7: Generate summary statistics
            self.generate_summary_statistics()
            
            # Step 8: Save to Excel
            self.save_to_excel(output_filename)
            
            return self.summary_stats
            
        except Exception as e:
            print(f"❌ Error during analysis: {e}")
            print("Check your file paths and data format.")
            return None

# Usage Example and Main Execution
if __name__ == "__main__":
    # File paths - UPDATE THESE WITH YOUR ACTUAL FILE PATHS
    ACD_FILE_PATH = "acd_1_8_.xls"  # Update with your ACD file path
    CALL_DETAILS_FILE_PATH = "Call_details_record_1_8_.xls"  # Update with your Call Details Record file path
    OUTPUT_FILE = "abandon_recovery_comprehensive_analysis.xlsx"
    
    try:
        # Initialize analyzer
        analyzer = AbandonRecoveryAnalyzer(ACD_FILE_PATH, CALL_DETAILS_FILE_PATH)
        
        # Run complete analysis
        results = analyzer.run_complete_analysis(OUTPUT_FILE)
        
        if results:
            # Display summary
            print("\n" + "=" * 60)
            print("ANALYSIS SUMMARY")
            print("=" * 60)
            for key, value in results.items():
                print(f"{key}: {value}")
            
            print(f"\n✅ Complete analysis saved to: {OUTPUT_FILE}")
        else:
            print("\n❌ Analysis failed. Please check the error messages above.")
        
    except FileNotFoundError as e:
        print(f"❌ Error: File not found - {e}")
        print("Please update the file paths in the script with your actual file locations.")
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        print("Please check your file formats and data structure.")