import json

# create company_datasource
# VoC
new_company_datasource_payload_voc_1 = {
    "type": "company_datasource",
    "company_datasource_id": 12,
    "company_id": 34,
    "company_name": "Google Pay",
    "source_id": 56,
    "source_name": "Capterra",
    "source_code": "Capterra",
    "source_type": "web",
    "urls": [{"url": "www.Capterra.com", "type": "review"}],
    "nlp_type": "VoC",
    "translation_service": "Google Translation"
}
new_company_datasource_payload_voc_2 = {
    "type": "company_datasource",
    "company_datasource_id": 22,
    "company_id": 5,
    "company_name": "Google Pay 2",
    "source_id": 516,
    "source_name": "G2",
    "source_code": "G2",
    "source_type": "web",
    "urls": [{"url": "www.g2.com", "type": "review"}],
    "nlp_type": "VoC",
    "translation_service": "Google Translation"
}
# VoE
new_company_datasource_payload_voe_1 = {
    "type": "company_datasource",
    "company_datasource_id": 13,
    "company_id": 12,
    "company_name": "Apple",
    "source_id": 14,
    "source_name": "LinkedIn",
    "source_code": "LinkedIn",
    "source_type": "web",
    "urls": [
        {"url": "www.LinkedIn.com", "type": "review"}, 
        {"url": "www.LinkedIn.com", "type": "overview"},
        {"url": "www.LinkedIn.com", "type": "job"}
    ],
    "nlp_type": "VoE",
    "translation_service": "Google Translation"
}
new_company_datasource_payload_voe_2 = {
    "type": "company_datasource",
    "company_datasource_id": 23,
    "company_id": 30,
    "company_name": "Amazon",
    "source_id": 7,
    "source_name": "Indeed",
    "source_code": "Indeed",
    "source_type": "web",
    "urls": [
        {"url": "www.indeed.com", "type": "review"}, 
        {"url": "www.indeed.com", "type": "overview"},
        {"url": "www.indeed.com", "type": "job"}
    ],
    "nlp_type": "VoE",
    "translation_service": "Google Translation"
}
# handle_postgres_company_datasource(payload=new_company_datasource_payload_voc_1)
# handle_postgres_company_datasource(payload=new_company_datasource_payload_voc_2)
# handle_postgres_company_datasource(payload=new_company_datasource_payload_voe_1)
# handle_postgres_company_datasource(payload=new_company_datasource_payload_voe_2)


# create case_study
sync_cs_payload_no_recrawl_voc = {
    "type": "case_study",
    "action": "sync",
    "case_study_id": 315,
    "case_study_name": "STAG Case Study Test",
    "clone_from_cs_id": None,
    "company_datasources": [
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 12,
        "is_target": True,
        "company_aliases": ["gpay", "gp"],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    },
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 22,
        "is_target": False,
        "company_aliases": [],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    }
    ],
    "relative_relevance": 0,
    "absolute_relevance": 0,
    "dimension_config": ['placeholder'], # same
    "exports": ['placeholder'], # same
    "polarities": ['placeholder'], # same
    "schedule_time": {'key': 'placeholder'} # same
    }

clone_cs_payload_no_recrawl_voc = {
    "type": "case_study",
    "action": "sync",
    "case_study_id": 515,
    "case_study_name": "STAG Case Study Test",
    "clone_from_cs_id": 315,
    "company_datasources": [
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 12,
        "is_target": True,
        "company_aliases": ["gpay", "gp"],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    },
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 22,
        "is_target": False,
        "company_aliases": [],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    }
    ],
    "relative_relevance": 0,
    "absolute_relevance": 0,
    "dimension_config": ['placeholder'], # same
    "exports": ['placeholder'], # same
    "polarities": ['placeholder'], # same
    "schedule_time": {'key': 'placeholder'} # same
    }

dimension_change_cs_payload_no_recrawl_voc = {
    "type": "case_study",
    "action": "dimension_change",
    "case_study_id": 315,
    "case_study_name": "STAG Case Study Test",
    "clone_from_cs_id": 315,
    "company_datasources": [
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 12,
        "is_target": True,
        "company_aliases": ["gpay", "gp"],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    },
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 22,
        "is_target": False,
        "company_aliases": [],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    }
    ],
    "relative_relevance": 0,
    "absolute_relevance": 0,
    "dimension_config": ['placeholder'], # same
    "exports": ['placeholder'], # same
    "polarities": ['placeholder'], # same
    "schedule_time": {'key': 'placeholder'} # same
    }

update_cs_payload_no_recrawl_voc = {
    "type": "case_study",
    "action": "update",
    "case_study_id": 315,
    "case_study_name": "STAG Case Study Test",
    "clone_from_cs_id": 315,
    "company_datasources": [
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 12,
        "is_target": True,
        "company_aliases": ["gpay", "gp"],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    },
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 22,
        "is_target": False,
        "company_aliases": [],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    }
    ],
    "relative_relevance": 0,
    "absolute_relevance": 0,
    "dimension_config": ['placeholder'], # same
    "exports": ['placeholder'], # same
    "polarities": ['placeholder'], # same
    "schedule_time": {'key': 'placeholder'} # same
    }

sync_cs_payload_no_recrawl_voe = {
    "type": "case_study",
    "action": "sync",
    "case_study_id": 415,
    "case_study_name": "STAG Case Study VOE",
    "clone_from_cs_id": None,
    "company_datasources": [
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 13,
        "is_target": True,
        "company_aliases": ["gpay", "gp"],
        "nlp_pack": "VoE Generic",
        "nlp_type": "VoE",
    },
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 23,
        "is_target": False,
        "company_aliases": [],
        "nlp_pack": "VoE Generic",
        "nlp_type": "VoE",
    }
    ],
    "relative_relevance": 0,
    "absolute_relevance": 0,
    "dimension_config": ['placeholder'], # same
    "exports": ['placeholder'], # same
    "polarities": ['placeholder'], # same
    "schedule_time": {'key': 'placeholder'} # same
    }

sync_cs_payload_recrawl_voc = {
    "type": "case_study",
    "action": "sync",
    "case_study_id": 316,
    "case_study_name": "STAG Case Study Test",
    "clone_from_cs_id": None,
    "company_datasources": [
    {
        "type": "company_datasource",
        "re_scrape": True,
        "re_nlp": False,
        "company_datasource_id": 12,
        "is_target": True,
        "company_aliases": ["gpay", "gp"],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    },
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 22,
        "is_target": False,
        "company_aliases": [],
        "nlp_pack": "VoC Generic",
        "nlp_type": "VoC",
    }
    ],
    "relative_relevance": 0,
    "absolute_relevance": 0,
    "dimension_config": ['placeholder'], # same
    "exports": ['placeholder'], # same
    "polarities": ['placeholder'], # same
    "schedule_time": {'key': 'placeholder'} # same
    }

sync_cs_payload_recrawl_voe = {
    "type": "case_study",
    "action": "sync",
    "case_study_id": 416,
    "case_study_name": "STAG Case Study Test",
    "clone_from_cs_id": None,
    "company_datasources": [
    {
        "type": "company_datasource",
        "re_scrape": False,
        "re_nlp": False,
        "company_datasource_id": 13,
        "is_target": True,
        "company_aliases": ["gpay", "gp"],
        "nlp_pack": "VoE Generic",
        "nlp_type": "VoE",
    },
    {
        "type": "company_datasource",
        "re_scrape": True,
        "re_nlp": False,
        "company_datasource_id": 23,
        "is_target": False,
        "company_aliases": [],
        "nlp_pack": "VoE Generic",
        "nlp_type": "VoE",
    }
    ],
    "relative_relevance": 0,
    "absolute_relevance": 0,
    "dimension_config": ['placeholder'], # same
    "exports": ['placeholder'], # same
    "polarities": ['placeholder'], # same
    "schedule_time": {'key': 'placeholder'} # same
    }

# handle_postgres_case_study(sync_cs_payload_no_recrawl_voc)
# handle_postgres_case_study(sync_cs_payload_no_recrawl_voe)
# handle_postgres_case_study(sync_cs_payload_recrawl_voc)
# handle_postgres_case_study(sync_cs_payload_recrawl_voe)

# update_cs_data
update_cs_data_progress_payload = {
    "type": "update_cs_data",
    "event": "progress",
    "case_study_id": 315,
    "postprocess_id": 17,
    "progress": 0.5,
}
update_cs_data_finish_payload = {
    "type": "update_cs_data",
    "event": "finish",
    "case_study_id": 315,
    "postprocess_id": 17,
    "error": "",
}
update_cs_data_fail_payload = {
    "type": "update_cs_data",
    "event": "fail",
    "case_study_id": 315,
    "postprocess_id": 17,
    "error": "ErrorType: some errors happened",
}

# nlp
nlp_progress_payload = {
    "type": "nlp",
    "event": "progress",
    "case_study_id": 315,
    "postprocess_id": 16,
    "progress": 0.5,
}
nlp_finish_payload = {
    "type": "nlp",
    "event": "finish",
    "case_study_id": 315,
    "postprocess_id": 16,
    "error": "",
}
nlp_fail_payload = {
    "type": "nlp",
    "event": "fail",
    "case_study_id": 315,
    "postprocess_id": 16,
    "error": "ErrorType: some errors happened",
}

# keyword_extract
keyword_extract_progress_payload = {
    "type": "keyword_extract",
    "event": "progress",
    "case_study_id": 315,
    "postprocess_id": 18,
    "progress": 0.5,
}
keyword_extract_finish_payload = {
    "type": "keyword_extract",
    "event": "finish",
    "case_study_id": 315,
    "postprocess_id": 18,
    "error": "",
}
keyword_extract_fail_payload = {
    "type": "keyword_extract",
    "event": "fail",
    "case_study_id": 315,
    "postprocess_id": 18,
    "error": "ErrorType: some errors happened",
}

# export
export_progress_payload = {
    "type": "export",
    "event": "progress",
    "case_study_id": 315,
    "postprocess_id": 19,
    "progress": 0.5,
}
export_finish_payload = {
    "type": "export",
    "event": "finish",
    "case_study_id": 315,
    "postprocess_id": 19,
    "error": "",
}
export_fail_payload = {
    "type": "export",
    "event": "fail",
    "case_study_id": 315,
    "postprocess_id": 19,
    "error": "ErrorType: some errors happened",
}

# word_frequency
word_frequency_progress_payload = {
    "type": "word_frequency",
    "event": "progress",
    "case_study_id": 315,
    "postprocess_id": 20,
    "progress": 0.5,
}
word_frequency_finish_payload = {
    "type": "word_frequency",
    "event": "finish",
    "case_study_id": 315,
    "postprocess_id": 20,
    "error": "",
}
word_frequency_fail_payload = {
    "type": "word_frequency",
    "event": "fail",
    "case_study_id": 315,
    "postprocess_id": 20,
    "error": "ErrorType: some errors happened",
}

# nlp_index
nlp_index_progress_payload = {
    "type": "nlp_index",
    "event": "progress",
    "case_study_id": 315,
    "postprocess_id": 21,
    "progress": 0.8,
}
nlp_index_finish_payload = {
    "type": "nlp_index",
    "event": "finish",
    "case_study_id": 315,
    "postprocess_id": 21,
    "error": "",
}
nlp_index_fail_payload = {
    "type": "nlp_index",
    "event": "fail",
    "case_study_id": 315,
    "postprocess_id": 21,
    "error": "ErrorType: some errors happened",
}

print(json.dumps(new_company_datasource_payload_voc_1))