"""
Enhanced Report Generator for Combined Futures + Options Analysis
with Historical Data Context
"""

import datetime
import pandas as pd
from config.settings import OUT_DIR

class ReportGenerator:
    def __init__(self):
        self.today = datetime.date.today()
    
    def generate_enhanced_reports(self, opportunities_df, futures_df, options_df, current_date, prev_date, historical_status):
        """Generate enhanced reports with historical data context"""
        
        if len(opportunities_df) > 0:
            # Save enhanced CSV
            self.save_enhanced_csv(opportunities_df)
            
            # Generate detailed report with historical context
            detailed_report = self.generate_historical_analysis_report(opportunities_df, futures_df, options_df, current_date, prev_date, historical_status)
            self.save_detailed_report(detailed_report)
            
            # Generate data quality summary
            quality_report = self.generate_data_quality_report(opportunities_df)
            self.save_quality_report(quality_report)
            
            # Generate executive summary
            summary_report = self.generate_executive_summary(opportunities_df)
            self.save_summary_report(summary_report)
            
            # Generate trading dashboard
            dashboard = self.generate_trading_dashboard(opportunities_df)
            self.save_dashboard(dashboard)
            
            print(f"📈 Generated {len(opportunities_df)} enhanced opportunities")
        else:
            self.save_no_opportunities_enhanced_report(current_date, prev_date, historical_status)
            print("📉 No enhanced opportunities found today")
    
    def generate_historical_analysis_report(self, opportunities_df, futures_df, options_df, current_date, prev_date, historical_status):
        """Generate report with historical data analysis"""
        
        report = []
        report.append("NSE ENHANCED COMBINED ANALYSIS WITH HISTORICAL DATA")
        report.append("=" * 70)
        report.append(f"Current Analysis Date: {current_date}")
        report.append(f"Previous Data Date: {prev_date if prev_date else 'Not Available'}")
        report.append(f"Historical Status: {historical_status}")
        report.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Analysis Type: COMBINED Futures + Options")
        report.append("")
        
        # Market Data Summary
        total_futures = len(futures_df) if futures_df is not None else 0
        total_options = len(options_df) if options_df is not None else 0
        analyzed_symbols = opportunities_df['symbol'].nunique() if len(opportunities_df) > 0 else 0
        
        report.append("MARKET DATA SUMMARY:")
        report.append(f"- Futures Contracts Analyzed: {total_futures}")
        report.append(f"- Options Contracts Analyzed: {total_options}")
        report.append(f"- Symbols with Complete Data: {analyzed_symbols}")
        report.append("")
        
        # Data Quality Summary
        if len(opportunities_df) > 0:
            high_quality = len(opportunities_df[opportunities_df['data_quality'] == 'HIGH'])
            medium_quality = len(opportunities_df[opportunities_df['data_quality'] == 'MEDIUM'])
            low_quality = len(opportunities_df[opportunities_df['data_quality'] == 'LOW'])
            with_historical = len(opportunities_df[opportunities_df['has_historical_data'] == True])
            
            report.append("DATA QUALITY SUMMARY:")
            report.append(f"- High Quality (Historical Data): {high_quality}")
            report.append(f"- Medium Quality: {medium_quality}")
            report.append(f"- Low Quality (Current Day Only): {low_quality}")
            report.append(f"- Total with Historical Data: {with_historical}")
            report.append("")
        
        # Opportunities Summary
        if len(opportunities_df) > 0:
            total_opportunities = len(opportunities_df)
            high_conf = len(opportunities_df[opportunities_df['confidence'] == 'High'])
            medium_conf = len(opportunities_df[opportunities_df['confidence'] == 'Medium'])
            call_opportunities = len(opportunities_df[opportunities_df['recommendation'].str.contains('CALL')])
            put_opportunities = len(opportunities_df[opportunities_df['recommendation'].str.contains('PUT')])
            
            report.append("FINAL TRADING VERDICT:")
            report.append(f"- Total Opportunities: {total_opportunities}")
            report.append(f"- High Confidence: {high_conf}")
            report.append(f"- Medium Confidence: {medium_conf}")
            report.append(f"- CALL Recommendations: {call_opportunities}")
            report.append(f"- PUT Recommendations: {put_opportunities}")
            report.append("")
            
            # Enhanced opportunities with data quality
            report.append("ENHANCED TRADING OPPORTUNITIES (with Data Quality):")
            report.append("=" * 60)
            
            # High Confidence Opportunities
            high_conf_opps = opportunities_df[opportunities_df['confidence'] == 'High']
            if len(high_conf_opps) > 0:
                report.append("🔥 HIGH CONFIDENCE TRADES (STRONG SIGNALS):")
                report.append("-" * 50)
                
                for _, opp in high_conf_opps.iterrows():
                    quality_icon = "🔥" if opp['data_quality'] == 'HIGH' else "⚠️" if opp['data_quality'] == 'MEDIUM' else "🔍"
                    historical_icon = "✅" if opp['has_historical_data'] else "⚠️"
                    
                    report.append(f"{quality_icon} {historical_icon} {opp['symbol']} - {opp['setup']}")
                    report.append(f"   RECOMMENDATION: {opp['recommendation']}")
                    report.append(f"   Data Quality: {opp['data_quality']} | Historical Data: {'Available' if opp['has_historical_data'] else 'Current Day Only'}")
                    report.append(f"   Current Price: {opp['current_price']:.2f}")
                    report.append(f"   Price Change: {opp['price_change_pct']:+.2f}%")
                    report.append(f"   OI Change: {opp['oi_change_pct']:+.1f}%")
                    report.append(f"   Volume Change: {opp['volume_change_pct']:+.1f}%")
                    report.append(f"   Call OI Trend: {opp['call_oi_change']:+.0f}")
                    report.append(f"   Put OI Trend: {opp['put_oi_change']:+.0f}")
                    if opp['max_pain']:
                        report.append(f"   Max Pain: {opp['max_pain']}")
                    report.append(f"   Strike: {opp['strike_guidance']}")
                    report.append(f"   Reason: {opp['reason']}")
                    report.append("")
            
            # Medium Confidence Opportunities
            medium_conf_opps = opportunities_df[opportunities_df['confidence'] == 'Medium']
            if len(medium_conf_opps) > 0:
                report.append("⚠️ MEDIUM CONFIDENCE TRADES (MODERATE SIGNALS):")
                report.append("-" * 50)
                
                for _, opp in medium_conf_opps.iterrows():
                    quality_icon = "🔥" if opp['data_quality'] == 'HIGH' else "⚠️" if opp['data_quality'] == 'MEDIUM' else "🔍"
                    historical_icon = "✅" if opp['has_historical_data'] else "⚠️"
                    
                    report.append(f"{quality_icon} {historical_icon} {opp['symbol']} - {opp['setup']}")
                    report.append(f"   RECOMMENDATION: {opp['recommendation']}")
                    report.append(f"   Data Quality: {opp['data_quality']} | Historical Data: {'Available' if opp['has_historical_data'] else 'Current Day Only'}")
                    report.append(f"   Current Price: {opp['current_price']:.2f}")
                    report.append(f"   Price Change: {opp['price_change_pct']:+.2f}%")
                    report.append(f"   Strike: {opp['strike_guidance']}")
                    report.append(f"   Reason: {opp['reason']}")
                    report.append("")
        
        else:
            report.append("❌ NO TRADING OPPORTUNITIES IDENTIFIED")
            report.append("")
            report.append("Possible reasons:")
            report.append("- Insufficient data for combined analysis")
            report.append("- No strong convergence between futures and options signals")
            report.append("- Market conditions not favorable for option buying")
            if historical_status == "UNAVAILABLE":
                report.append("- First run: No historical data for comparison")
                report.append("- 💡 Run again tomorrow for historical data analysis")
            else:
                report.append("- Try analysis during active market hours")
        
        # Analysis Methodology
        report.append("ANALYSIS METHODOLOGY:")
        report.append("=" * 60)
        report.append("✅ ENHANCED COMBINED FUTURES + OPTIONS ANALYSIS:")
        report.append("   • Futures Data: Price trend, OI buildup, Volume analysis")
        report.append("   • Options Data: OI patterns, Volume spikes, Max Pain")
        report.append("   • Historical Comparison: Previous vs Current day data")
        report.append("   • Combined Verdict: Both datasets must confirm direction")
        report.append("")
        report.append("📊 DATA QUALITY LEVELS:")
        report.append("   • HIGH: Both previous and current day data available")
        report.append("   • MEDIUM: Some historical data available") 
        report.append("   • LOW: Current day data only (first run or missing data)")
        report.append("")
        report.append("🎯 CONFIDENCE LEVELS:")
        report.append("   • HIGH: Strong convergence in both futures and options")
        report.append("   • MEDIUM: Moderate signals with some confirmation")
        report.append("")
        report.append("⚡ TRADING GUIDELINES:")
        report.append("   • Use strict stop losses (20-25% for options)")
        report.append("   • Position size: 2-5% of capital per trade")
        report.append("   • Prefer near-month expiry for better gamma")
        report.append("   • Avoid last 3 days of expiry for swing positions")
        report.append("   • Focus on HIGH data quality signals when available")
        
        return "\n".join(report)
    
    def save_enhanced_csv(self, opportunities_df):
        """Save enhanced opportunities to CSV"""
        csv_path = f"{OUT_DIR}/signals/enhanced_opportunities_{self.today}.csv"
        opportunities_df.to_csv(csv_path, index=False)
        print(f"💾 Enhanced CSV report saved: {csv_path}")
    
    def save_detailed_report(self, report_content):
        """Save detailed report"""
        report_path = f"{OUT_DIR}/reports/enhanced_analysis_{self.today}.txt"
        with open(report_path, 'w') as f:
            f.write(report_content)
        print(f"📄 Enhanced detailed report saved: {report_path}")
    
    def generate_data_quality_report(self, opportunities_df):
        """Generate data quality analysis report"""
        report = []
        report.append("DATA QUALITY ANALYSIS REPORT")
        report.append("=" * 40)
        report.append(f"Date: {self.today}")
        report.append("")
        
        if len(opportunities_df) > 0:
            by_quality = opportunities_df['data_quality'].value_counts()
            by_confidence = opportunities_df['confidence'].value_counts()
            historical_count = opportunities_df['has_historical_data'].sum()
            
            report.append("QUALITY DISTRIBUTION:")
            for quality, count in by_quality.items():
                report.append(f"- {quality}: {count}")
            
            report.append("")
            report.append("CONFIDENCE DISTRIBUTION:")
            for confidence, count in by_confidence.items():
                report.append(f"- {confidence}: {count")
            
            report.append("")
            report.append(f"With Historical Data: {historical_count}/{len(opportunities_df)}")
            report.append("")
            report.append("RECOMMENDATION:")
            if historical_count > 0:
                report.append("✅ Focus on HIGH quality signals with historical data")
            else:
                report.append("⚠️ All signals based on current day data only")
                report.append("💡 Run again tomorrow for historical comparison")
        else:
            report.append("No opportunities to analyze")
        
        return "\n".join(report)
    
    def save_quality_report(self, quality_report):
        """Save quality report"""
        quality_path = f"{OUT_DIR}/reports/data_quality_{self.today}.txt"
        with open(quality_path, 'w') as f:
            f.write(quality_report)
        print(f"📊 Data quality report saved: {quality_path}")
    
    def generate_executive_summary(self, opportunities_df):
        """Generate executive summary for quick review"""
        summary = []
        summary.append(f"EXECUTIVE SUMMARY - ENHANCED ANALYSIS")
        summary.append("=" * 50)
        summary.append(f"Date: {self.today}")
        summary.append("")
        
        if len(opportunities_df) > 0:
            high_conf = opportunities_df[opportunities_df['confidence'] == 'High']
            call_trades = opportunities_df[opportunities_df['recommendation'].str.contains('CALL')]
            put_trades = opportunities_df[opportunities_df['recommendation'].str.contains('PUT')]
            high_quality = opportunities_df[opportunities_df['data_quality'] == 'HIGH']
            
            summary.append(f"Total Opportunities: {len(opportunities_df)}")
            summary.append(f"High Confidence: {len(high_conf)}")
            summary.append(f"High Data Quality: {len(high_quality)}")
            summary.append(f"CALL Trades: {len(call_trades)}")
            summary.append(f"PUT Trades: {len(put_trades)}")
            summary.append("")
            
            if len(high_conf) > 0:
                summary.append("TOP HIGH CONFIDENCE TRADES:")
                for _, opp in high_conf.iterrows():
                    quality_icon = "🔥" if opp['data_quality'] == 'HIGH' else "⚠️"
                    summary.append(f"{quality_icon} {opp['symbol']}: {opp['recommendation']} (Price: {opp['current_price']:.2f})")
        else:
            summary.append("No trading opportunities today.")
        
        return "\n".join(summary)
    
    def save_summary_report(self, summary_content):
        """Save summary report"""
        summary_path = f"{OUT_DIR}/reports/executive_summary_{self.today}.txt"
        with open(summary_path, 'w') as f:
            f.write(summary_content)
        print(f"📋 Executive summary saved: {summary_path}")
    
    def generate_trading_dashboard(self, opportunities_df):
        """Generate trading dashboard"""
        dashboard = []
        dashboard.append("TRADING DASHBOARD - ENHANCED ANALYSIS")
        dashboard.append("=" * 40)
        dashboard.append(f"Date: {self.today}")
        dashboard.append("")
        
        if len(opportunities_df) > 0:
            for _, opp in opportunities_df.iterrows():
                confidence_icon = "🔥" if opp['confidence'] == 'High' else "⚠️"
                direction_icon = "📈" if 'CALL' in opp['recommendation'] else "📉"
                quality_icon = "✅" if opp['data_quality'] == 'HIGH' else "🔍"
                dashboard.append(f"{quality_icon} {confidence_icon} {direction_icon} {opp['symbol']}: {opp['recommendation']}")
        else:
            dashboard.append("No trades recommended today.")
        
        return "\n".join(dashboard)
    
    def save_dashboard(self, dashboard_content):
        """Save trading dashboard"""
        dashboard_path = f"{OUT_DIR}/reports/trading_dashboard_{self.today}.txt"
        with open(dashboard_path, 'w') as f:
            f.write(dashboard_content)
        print(f"🎯 Trading dashboard saved: {dashboard_path}")
    
    def save_no_opportunities_enhanced_report(self, current_date, prev_date, historical_status):
        """Save enhanced report when no opportunities found"""
        report = []
        report.append("ENHANCED ANALYSIS REPORT - NO OPPORTUNITIES")
        report.append("=" * 50)
        report.append(f"Current Date: {current_date}")
        report.append(f"Previous Date: {prev_date if prev_date else 'Not Available'}")
        report.append(f"Historical Status: {historical_status}")
        report.append(f"Analysis Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("ANALYSIS RESULTS:")
        report.append("❌ No trading opportunities identified with current criteria")
        report.append("")
        report.append("POSSIBLE REASONS:")
        report.append("- Market conditions not favorable for option buying")
        report.append("- No strong convergence between futures and options signals")
        if historical_status == "UNAVAILABLE":
            report.append("- First run: No historical data for comparison")
            report.append("- 💡 Run again tomorrow for historical data analysis")
        else:
            report.append("- Try adjusting strategy parameters in config/settings.py")
        
        report_path = f"{OUT_DIR}/reports/enhanced_analysis_{self.today}.txt"
        with open(report_path, 'w') as f:
            f.write("\n".join(report))
        print(f"📄 Enhanced no-opportunities report saved: {report_path}")
