"""
Report generation module for option buying opportunities
"""

import datetime
import pandas as pd
from config.settings import OUT_DIR

class ReportGenerator:
    def __init__(self):
        self.today = datetime.date.today()
    
    def generate_all_reports(self, opportunities_df, futures_df, options_df, option_chain, underlying_prices):
        """Generate all report types"""
        
        if len(opportunities_df) > 0:
            # Save CSV report
            self.save_csv_report(opportunities_df)
            
            # Generate text report
            text_report = self.generate_text_report(opportunities_df, futures_df, options_df, option_chain, underlying_prices)
            self.save_text_report(text_report)
            
            # Generate summary report
            summary_report = self.generate_summary_report(opportunities_df)
            self.save_summary_report(summary_report)
            
            print(f"📈 Found {len(opportunities_df)} opportunities")
        else:
            self.save_empty_report()
            print("📉 No opportunities found today")
    
    def save_csv_report(self, opportunities_df):
        """Save opportunities to CSV"""
        csv_path = f"{OUT_DIR}/signals/option_opportunities_{self.today}.csv"
        opportunities_df.to_csv(csv_path, index=False)
        print(f"💾 CSV report saved: {csv_path}")
    
    def generate_text_report(self, opportunities_df, futures_df, options_df, option_chain, underlying_prices):
        """Generate detailed text report"""
        
        report = []
        report.append("NSE OPTION BUYING STRATEGY REPORT")
        report.append("=" * 70)
        report.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Opportunities: {len(opportunities_df)}")
        report.append("")
        
        # Market overview
        total_futures = len(futures_df)
        total_options = len(options_df)
        unique_underlyings = options_df['underlying'].nunique() if len(options_df) > 0 else 0
        
        report.append("MARKET OVERVIEW:")
        report.append(f"- Futures Contracts: {total_futures}")
        report.append(f"- Options Contracts: {total_options}")
        report.append(f"- Unique Underlyings: {unique_underlyings}")
        
        if len(options_df) > 0:
            option_type_dist = options_df['option_type'].value_counts()
            report.append(f"- Call Options: {option_type_dist.get('CE', 0)}")
            report.append(f"- Put Options: {option_type_dist.get('PE', 0)}")
        report.append("")
        
        # High confidence opportunities
        high_conf = opportunities_df[opportunities_df['confidence'] == 'High']
        if len(high_conf) > 0:
            report.append("🔥 HIGH CONFIDENCE OPPORTUNITIES:")
            report.append("-" * 50)
            for _, opp in high_conf.iterrows():
                report.append(f"• {opp['symbol']} - {opp['setup']}")
                report.append(f"  Recommendation: {opp['recommendation']}")
                report.append(f"  Price: {opp.get('future_price', opp.get('current_price', 'N/A')):.2f}")
                if 'price_change_pct' in opp:
                    report.append(f"  Change: {opp['price_change_pct']:+.2f}%")
                if 'max_pain' in opp:
                    report.append(f"  Max Pain: {opp['max_pain']}")
                report.append(f"  Strike: {opp['strike_guidance']}")
                report.append(f"  Reason: {opp['reason']}")
                report.append("")
        
        # All opportunities summary
        report.append("ALL OPPORTUNITIES SUMMARY:")
        report.append("-" * 50)
        for _, opp in opportunities_df.iterrows():
            report.append(f"• {opp['symbol']}: {opp['setup']} ({opp['confidence']}) - {opp['recommendation']}")
        report.append("")
        
        return "\n".join(report)
    
    def save_text_report(self, report_content):
        """Save text report to file"""
        report_path = f"{OUT_DIR}/reports/detailed_report_{self.today}.txt"
        with open(report_path, 'w') as f:
            f.write(report_content)
        print(f"📄 Detailed report saved: {report_path}")
    
    def generate_summary_report(self, opportunities_df):
        """Generate brief summary report"""
        summary = []
        summary.append(f"Option Opportunities Summary - {self.today}")
        summary.append("=" * 40)
        
        if len(opportunities_df) > 0:
            setup_counts = opportunities_df['setup'].value_counts()
            confidence_counts = opportunities_df['confidence'].value_counts()
            
            summary.append(f"Total Opportunities: {len(opportunities_df)}")
            summary.append("")
            summary.append("By Setup:")
            for setup, count in setup_counts.items():
                summary.append(f"  {setup}: {count}")
            
            summary.append("")
            summary.append("By Confidence:")
            for conf, count in confidence_counts.items():
                summary.append(f"  {conf}: {count}")
        else:
            summary.append("No opportunities found today")
        
        return "\n".join(summary)
    
    def save_summary_report(self, summary_content):
        """Save summary report"""
        summary_path = f"{OUT_DIR}/reports/summary_{self.today}.txt"
        with open(summary_path, 'w') as f:
            f.write(summary_content)
        print(f"📋 Summary report saved: {summary_path}")
    
    def save_empty_report(self):
        """Save empty report when no opportunities found"""
        empty_report = f"No option buying opportunities identified on {self.today}"
        report_path = f"{OUT_DIR}/reports/detailed_report_{self.today}.txt"
        with open(report_path, 'w') as f:
            f.write(empty_report)
        print(f"📄 Empty report saved: {report_path}")
