import pandas as pd
import numpy as np
import unittest
import math
from datetime import datetime
from bulum import stoch
from bulum import utils
from bulum import io
import bulum.stats as osta

class Tests(unittest.TestCase):

    def test_annual_max(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        answer_non_complete_wy = osta.annual_max(df,7,True)["rex_creek_sac_flow"]
        self.assertAlmostEqual(answer_non_complete_wy,6.68182359932205)

        #Test not working for some reason
        df = io.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv",r"%Y-%m-%d")
        answer_complete_wy = osta.annual_max(df,7)["Functions\\Functions\\Functions\\Functions@Results@ODH_RWA@$f_KurandaTWS (ML.day^-1)"]
        self.assertAlmostEqual(answer_complete_wy,459.819458676998)

    def test_annual_min(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        answer_non_complete_wy = osta.annual_min(df,7,True)["rex_creek_sac_flow"]
        self.assertAlmostEqual(answer_non_complete_wy,6.68182359932205)

        #Test not working for some reason
        df = io.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv",r"%Y-%m-%d")
        answer_complete_wy = osta.annual_min(df,7)["Functions\\Functions\\Functions\\Functions@Results@ODH_RWA@$f_KurandaTWS (ML.day^-1)"]
        self.assertAlmostEqual(answer_complete_wy,452.252343499997)

    def test_annual_mean(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        answer_non_complete_wy = osta.annual_mean(df,7,True)["rex_creek_sac_flow"]
        self.assertAlmostEqual(answer_non_complete_wy,6.68182359932205)

        #Test not working for some reason
        df = io.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv",r"%Y-%m-%d")
        answer_complete_wy = osta.annual_mean(df,7)["Functions\\Functions\\Functions\\Functions@Results@ODH_RWA@$f_KurandaTWS (ML.day^-1)"]
        self.assertAlmostEqual(answer_complete_wy,457.095643460864)

    def test_annual_median(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        answer_non_complete_wy = osta.annual_median(df,7,True)["rex_creek_sac_flow"]
        self.assertAlmostEqual(answer_non_complete_wy,6.68182359932205)

        #Test not working for some reason
        df = io.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv",r"%Y-%m-%d")
        answer_complete_wy = osta.annual_median(df,7)["Functions\\Functions\\Functions\\Functions@Results@ODH_RWA@$f_KurandaTWS (ML.day^-1)"]
        self.assertAlmostEqual(answer_complete_wy,457.233468094998)

    def test_monthly_reliability(self):
        # Test timeseries demand input
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp = osta.Reliability(df["Functions\\Functions\\Functions\\Functions@ODH@$f_Irrigator_180day_Unrestricted (ML.day^-1)"],df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], quiet=True)
        
        answer_non_complete_months = temp.MonthlyReliability(.99,True)
        self.assertAlmostEqual(answer_non_complete_months,0.863319386)
    
        answer_complete_months = temp.MonthlyReliability(.99)
        self.assertAlmostEqual(answer_complete_months,0.863128492)

        # Test monthly list of daily demands
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp = osta.Reliability([8.5,8.5,8.5,8.5,0,0,0,0,0,0,8.5,8.5],df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], demand_type="daily_constant", quiet=True)
        
        answer_non_complete_months_list = temp.MonthlyReliability(.99,True)
        self.assertAlmostEqual(answer_non_complete_months_list,0.863319386)
    
        answer_complete_months_list = temp.MonthlyReliability(.99)
        self.assertAlmostEqual(answer_complete_months_list,0.863128492)

        # Test monthly list of total demands
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_total = osta.Reliability([263.5,238,263.5,255,0,0,0,0,0,0,255,263.5],df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], demand_type="total", ignore_leap_years=True, quiet=True)

        answer_non_complete_months_total_list = temp_total.MonthlyReliability(.99,True)
        self.assertAlmostEqual(answer_non_complete_months_total_list,0.863319386)
    
        answer_complete_months_total_list = temp_total.MonthlyReliability(.99)
        self.assertAlmostEqual(answer_complete_months_total_list,0.863128492)

        # Test daily constant demand
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_day_constant = osta.Reliability(8.5,df["Random 8.5 Supply"], quiet=True)

        answer_non_complete_months_day_constant = temp_day_constant.MonthlyReliability(0.95,True)
        self.assertAlmostEqual(answer_non_complete_months_day_constant,0.566945607)

        answer_complete_months_day_constant = temp_day_constant.MonthlyReliability(0.95)
        self.assertAlmostEqual(answer_complete_months_day_constant,0.567039106)

        # Test annual constant demand
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_annual_constant = osta.Reliability(3102.5,df["Random 8.5 Supply"], ignore_leap_years=True, demand_timescale="yearly", demand_type="total", quiet=True)

        answer_non_complete_months_annual_constant = temp_annual_constant.MonthlyReliability(0.95,True)
        self.assertAlmostEqual(answer_non_complete_months_annual_constant,0.566945607)
    
        answer_complete_months_annual_constant = temp_annual_constant.MonthlyReliability(0.95)
        self.assertAlmostEqual(answer_complete_months_annual_constant,0.567039106)


    def test_annual_reliability(self):
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp = osta.Reliability(df["Functions\\Functions\\Functions\\Functions@ODH@$f_Irrigator_180day_Unrestricted (ML.day^-1)"],df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], quiet=True)
        
        answer_non_complete_years = temp.AnnualReliability(.99,7,True)
        self.assertAlmostEqual(answer_non_complete_years,0.641666667)
    
        answer_complete_years = temp.AnnualReliability(.99,7)
        self.assertAlmostEqual(answer_complete_years,0.644067797)       

        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp = osta.Reliability([8.5,8.5,8.5,8.5,0,0,0,0,0,0,8.5,8.5],df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], demand_type="daily_constant", quiet=True)
        
        answer_non_complete_years_list = temp.AnnualReliability(.99,7,True)
        self.assertAlmostEqual(answer_non_complete_years_list,0.641666667)
    
        answer_complete_years_list = temp.AnnualReliability(.99,7)
        self.assertAlmostEqual(answer_complete_years_list,0.644067797)

        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_total = osta.Reliability([263.5,238,263.5,255,0,0,0,0,0,0,255,263.5],df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], demand_type="total", ignore_leap_years=True, quiet=True)

        answer_non_complete_years_total_list = temp_total.AnnualReliability(.99,7,True)
        self.assertAlmostEqual(answer_non_complete_years_total_list,0.641666667)
    
        answer_complete_years_total_list = temp_total.AnnualReliability(.99,7)
        self.assertAlmostEqual(answer_complete_years_total_list,0.644067797)       

        # Test daily constant demand
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_day_constant = osta.Reliability(8.5,df["Random 8.5 Supply"], quiet=True)

        answer_non_complete_years_day_constant = temp_day_constant.AnnualReliability(0.95,7,True)
        self.assertAlmostEqual(answer_non_complete_years_day_constant,0.591666667)
    
        answer_complete_years_day_constant = temp_day_constant.AnnualReliability(0.95,7)
        self.assertAlmostEqual(answer_complete_years_day_constant,0.584745763)

        # Test annual constant demand
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_annual_constant = osta.Reliability(3102.5,df["Random 8.5 Supply"], ignore_leap_years=True, demand_timescale="yearly", demand_type="total", quiet=True)

        answer_non_complete_years_annual_constant = temp_annual_constant.AnnualReliability(0.95,7,True)
        self.assertAlmostEqual(answer_non_complete_years_annual_constant,0.591666667)
    
        answer_complete_years_annual_constant = temp_annual_constant.AnnualReliability(0.95,7)
        self.assertAlmostEqual(answer_complete_years_annual_constant,0.584745763)

    def test_storage_level_assessment(self):
        ## Checking answers against "GB_RCP45_2050_02b_StormKingDam.in" outputs in 26009
        
        ## Define a SLA at Storm King Dam
        df = io.read_ts_csv("./src/bulum/stats/tests/test_storage_data.csv")
        sla = osta.StorageLevelAssessment(df[r"Storage\0013 Storm King Dam\Storage Volume (ML)"],[400,655,1090,1530])

        # Test if the SLA calculates the correct events (ref "GB_RCP45_2050_01a_StormKingDam.in_Events.csv")
        answer_events=sla.EventsBelowTrigger()
        read_events={}
        read_events[400]=[75,4,64,103,9,289]
        read_events[655]=[154,104,24,188,238,411,56]
        read_events[1090]=[298,150,3,207,116,8,27,77,5,3,299,88,5,94,9,25,32,1,4,48,809,17,98,47,21,1,40,580,36,60,79,186]
        read_events[1530]=[3,443,2,95,255,4,147,52,355,28,232,179,174,269,2,8,104,165,202,584,142,92,230,21,5,1,57,144,124,310,2,50,99,107,147,204,481,63,984,2,12,137,533,1167,47,23,175,257,471]
        self.assertDictEqual(answer_events,read_events)

        # Spot testing individual results
        self.assertEqual(sla.EventsBelowTriggerMax()[655],411)
        self.assertEqual(sla.NumberWaterYearsBelow()[1530],61)
        self.assertAlmostEqual(sla.PercentWaterYearsBelow()[400],0.038461538)
        self.assertEqual(sla.EventsBelowTriggerCount(365)[1090],2)

        # Test annual table (ref "GB_RCP45_2050_01a_StormKingDam.in_AnnualDaysBelow.csv")
        answer_annual=sla.AnnualDaysBelowSummary()
        read_annual = pd.read_csv("./src/bulum/stats/tests/test_storage_data_annualdays_answers.csv",index_col=0)
        read_annual.columns=read_annual.columns.astype(int)
        pd.testing.assert_frame_equal(answer_annual,read_annual,check_dtype=False,check_index_type=False,check_column_type=False)
        
        # Test summary table (ref "GB_RCP45_2050_01a_StormKingDam.in_StorageAssessmentResults.csv")
        answer_summary=sla.Summary()
        read_summary=pd.read_csv("./src/bulum/stats/tests/test_storage_data_summary_answers.csv",index_col=0)
        pd.testing.assert_frame_equal(answer_summary,read_summary,check_dtype=False,check_index_type=False,check_column_type=False)

    def test_StochasticDataComparison(self):
        ## Checking answers against PPT Historical outputs in Checking_LF_B.xlsm in 10010
        
        ## Define an SDC
        df = io.read("./src/bulum/stats/tests/da_file/nogr306a.idx")
        sdc = osta.StochasticDataComparison({"Historical": df.loc[:"2011-06-30",:]})

        ## Test distributions
        check_dist_ann=sdc.Distributions["outputs"]["annual"]["7>PPT502Ba.q13>GS130502B Brown River at Brown Lake Part"]["Historical"].dropna().reset_index()
        read_dist_ann=pd.read_csv("./src/bulum/stats/tests/test_sdc_distr_ann_answers.csv")
        pd.testing.assert_frame_equal(round(check_dist_ann,4),round(read_dist_ann,4),check_dtype=False)

        check_dist_mon=sdc.Distributions["outputs"]["07"]["3>PPT_206A.q13>GS130206A Theresa Creek at Gregory Highw"]["Historical"].dropna().reset_index()
        read_dist_mon=pd.read_csv("./src/bulum/stats/tests/test_sdc_distr_mon_answers.csv")
        pd.testing.assert_frame_equal(round(check_dist_mon,4),round(read_dist_mon,4),check_dtype=False)

        ## Test correlations
        #Annual
        read_corr_ann=pd.read_csv("./src/bulum/stats/tests/test_sdc_corr_ann_answers.csv")
        check_corr_ann=sdc.Correlations["outputs"]["annual"]["lag0"]["15>PPT_413A.q13>GS130413A Denison Creek at Braeside"]["Historical"]
        pd.testing.assert_series_equal(round(check_corr_ann,3),read_corr_ann["Lag0"],check_index=False,check_names=False)

        check_corr_ann=sdc.Correlations["outputs"]["annual"]["lag1"]["15>PPT_413A.q13>GS130413A Denison Creek at Braeside"]["Historical"]
        pd.testing.assert_series_equal(round(check_corr_ann,3),read_corr_ann["Lag1"],check_index=False,check_names=False)
        #Monthly
        read_corr_mon=pd.read_csv("./src/bulum/stats/tests/test_sdc_corr_mon_answers.csv")
        check_corr_mon=sdc.Correlations["outputs"]["02"]["lag0"]["12>PPT_106A.q13>GS130106A Mackenzie River at Bingegang"]["Historical"]
        pd.testing.assert_series_equal(round(check_corr_mon,3),read_corr_mon["Lag0"],check_index=False,check_names=False)

        check_corr_mon=sdc.Correlations["outputs"]["02"]["lag1"]["12>PPT_106A.q13>GS130106A Mackenzie River at Bingegang"]["Historical"]
        pd.testing.assert_series_equal(round(check_corr_mon,3),read_corr_mon["Lag1"],check_index=False,check_names=False)

        ## Spot testing individual results
        self.assertEqual(round(sdc.Stats["outputs"]["annual"]["mean"]["Historical"]["20>PPT_410A.q13>GS130410A Isaac River at Deverill"],1),592.7)
        self.assertEqual(round(sdc.Stats["outputs"]["04"]["skew"]["Historical"]["20>PPT_410A.q13>GS130410A Isaac River at Deverill"],3),3.972)        

        ## Testing charts
        sdc.Stats["chts"]["annual"]["skew"]
        sdc.Correlations["chts"]["annual"]["lag1"]["3>PPT_206A.q13>GS130206A Theresa Creek at Gregory Highw"]
        sdc.Correlations["heatmaps"]["annual"]["lag0"]["Historical"]
        sdc.Distributions["chts"]["annual"]["7>PPT502Ba.q13>GS130502B Brown River at Brown Lake Part"]

    def test_cumulative_risk(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            #ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(osta.cumulative_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=[100000,64000]).__len__(),2)
        self.assertAlmostEqual(osta.cumulative_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=[100000,64000])["64000"].mean(),6.572572150832104)
        self.assertEqual(osta.cumulative_risk(input=ensemble,variable="Storage",parameters=[64000]).__len__(),1)
        self.assertEqual(osta.cumulative_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=64000).__len__(),1)

    def test_incremental_risk(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            #ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(osta.incremental_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=[100000,64000]).__len__(),2)
        self.assertAlmostEqual(osta.incremental_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=[100000,64000])["64000"].mean(),1.3692858647566883)
        self.assertEqual(osta.incremental_risk(input=ensemble,variable="Storage",parameters=[64000]).__len__(),1)
        self.assertEqual(osta.incremental_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=64000).__len__(),1)
        
    def test_annual_incremental_risk(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            #ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(osta.annual_incremental_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=[100000,64000]).__len__(),2)
        self.assertAlmostEqual(osta.annual_incremental_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=[100000,64000])["64000"].mean(),3.8461538461538463)
        self.assertEqual(osta.annual_incremental_risk(input=ensemble,variable="Storage",parameters=[64000]).__len__(),1)
        self.assertEqual(osta.annual_incremental_risk(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=64000).__len__(),1)

    def test_percentile_envelope(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            #ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(osta.percentile_envelope(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=[0,10,25,50]).__len__(),4)
        self.assertAlmostEqual(osta.percentile_envelope(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=[0,10,25,50])["10"].mean(),130487.5073260038)
        self.assertEqual(osta.percentile_envelope(input=ensemble,variable="Storage",parameters=[10]).__len__(),1)
        self.assertEqual(osta.percentile_envelope(input=ensemble.filter_tag("scen1"),variable="Storage",parameters=10).__len__(),1)
        

if __name__ == '__main__':
    unittest.main()
