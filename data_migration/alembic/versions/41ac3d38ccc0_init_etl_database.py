"""init etl database

Revision ID: 41ac3d38ccc0
Revises: 
Create Date: 2021-01-06 16:52:16.469045

"""
import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = '41ac3d38ccc0'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(INIT_SQL)
    connection.commit()
    cur.close()
    connection.close()


def downgrade():
    pass


INIT_SQL = """
CREATE SCHEMA mdm;

CREATE TABLE mdm.etl_company_batch (
    request_id integer,
    batch_name character varying(255),
    status character varying(255),
    company_name character varying,
    source_name character varying,
    url character varying,
    created_at timestamp without time zone,
    batch_id integer NOT NULL,
    updated_at timestamp without time zone,
    nlp_pack character varying(255),
    nlp_type character varying(255),
    company_id integer,
    source_id integer,
    source_code character varying
);


ALTER TABLE mdm.etl_company_batch OWNER TO dpadmin;

--
-- TOC entry 3703 (class 0 OID 0)
-- Dependencies: 203
-- Name: COLUMN etl_company_batch.batch_name; Type: COMMENT; Schema: mdm; Owner: dpadmin
--

COMMENT ON COLUMN mdm.etl_company_batch.batch_name IS 'A batch name contains a maximum of 67 characters, created by a company name, a source name, and date in yyyymmdd format. Each information is not larger than 10 characters.';


--
-- TOC entry 204 (class 1259 OID 16435)
-- Name: EtlCompanyBatch_batch_id_seq; Type: SEQUENCE; Schema: mdm; Owner: dpadmin
--

CREATE SEQUENCE mdm."EtlCompanyBatch_batch_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mdm."EtlCompanyBatch_batch_id_seq" OWNER TO dpadmin;

--
-- TOC entry 3704 (class 0 OID 0)
-- Dependencies: 204
-- Name: EtlCompanyBatch_batch_id_seq; Type: SEQUENCE OWNED BY; Schema: mdm; Owner: dpadmin
--

ALTER SEQUENCE mdm."EtlCompanyBatch_batch_id_seq" OWNED BY mdm.etl_company_batch.batch_id;


--
-- TOC entry 205 (class 1259 OID 16447)
-- Name: etl_step; Type: TABLE; Schema: mdm; Owner: dpadmin
--

CREATE TABLE mdm.etl_step (
    batch_id integer,
    request_id integer,
    status character varying,
    step_name character varying,
    step_id integer NOT NULL,
    created_at timestamp without time zone,
    step_type character varying(255),
    updated_at timestamp without time zone
);


ALTER TABLE mdm.etl_step OWNER TO dpadmin;

--
-- TOC entry 206 (class 1259 OID 16453)
-- Name: EtlStep_step_id_seq; Type: SEQUENCE; Schema: mdm; Owner: dpadmin
--

CREATE SEQUENCE mdm."EtlStep_step_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mdm."EtlStep_step_id_seq" OWNER TO dpadmin;

--
-- TOC entry 3705 (class 0 OID 0)
-- Dependencies: 206
-- Name: EtlStep_step_id_seq; Type: SEQUENCE OWNED BY; Schema: mdm; Owner: dpadmin
--

ALTER SEQUENCE mdm."EtlStep_step_id_seq" OWNED BY mdm.etl_step.step_id;


--
-- TOC entry 214 (class 1259 OID 16484)
-- Name: dq_storage; Type: TABLE; Schema: mdm; Owner: dpadmin
--

CREATE TABLE mdm.dq_storage (
    id bigint,
    folder text,
    batch text,
    file text,
    batch_format_check text,
    step_detail_format_check text,
    created_at text
);


ALTER TABLE mdm.dq_storage OWNER TO dpadmin;

--
-- TOC entry 218 (class 1259 OID 17306)
-- Name: etl_request; Type: TABLE; Schema: mdm; Owner: dpadmin
--

CREATE TABLE mdm.etl_request (
    request_id integer NOT NULL,
    case_study_id integer,
    case_study_name character varying(255),
    status character varying(255),
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);


ALTER TABLE mdm.etl_request OWNER TO dpadmin;

--
-- TOC entry 215 (class 1259 OID 16490)
-- Name: etl_step_detail; Type: TABLE; Schema: mdm; Owner: dpadmin
--

CREATE TABLE mdm.etl_step_detail (
    request_id integer,
    step_detail_name character(500),
    status character varying,
    step_id integer,
    batch_id integer,
    file_id integer,
    paging character varying,
    created_at timestamp without time zone,
    step_detail_id integer NOT NULL,
    updated_at timestamp without time zone,
    progress_current integer DEFAULT 0,
    progress_total integer DEFAULT 0,
    item_count integer,
    lang character(8),
    filepath character varying(255),
    ref_id integer
);


ALTER TABLE mdm.etl_step_detail OWNER TO dpadmin;

--
-- TOC entry 3706 (class 0 OID 0)
-- Dependencies: 215
-- Name: COLUMN etl_step_detail.paging; Type: COMMENT; Schema: mdm; Owner: dpadmin
--

COMMENT ON COLUMN mdm.etl_step_detail.paging IS 'applestore: web has "mouse rolling" for paging, paging represents the number of "mouse rolling" 
googleplay: web has "mouse rolling" for paging, paging represents for the number of "mouse rolling" 
capterra: web has page number for paging, paging represents for the number of page is crawled.';


--
-- TOC entry 220 (class 1259 OID 17334)
-- Name: check_file_not_on_step_detail; Type: VIEW; Schema: mdm; Owner: dpadmin
--

CREATE VIEW mdm.check_file_not_on_step_detail AS
 WITH cte_etl AS (
         SELECT t1.request_id,
            t1.case_study_name,
            t2.batch_id,
            t2.batch_name,
            t3.step_id,
            t3.step_name,
            "substring"((t3.step_name)::text, 1, ("position"((t3.step_name)::text, '-'::text) - 1)) AS job,
            t4.step_detail_name
           FROM (((mdm.etl_request t1
             LEFT JOIN mdm.etl_company_batch t2 ON ((t1.request_id = t2.request_id)))
             LEFT JOIN mdm.etl_step t3 ON ((t3.batch_id = t2.batch_id)))
             LEFT JOIN mdm.etl_step_detail t4 ON ((t4.step_id = t3.step_id)))
          ORDER BY t1.request_id DESC, t2.batch_id DESC, t3.step_id, t4.step_detail_id
        ), cte AS (
         SELECT st.id,
            st.folder,
            st.batch,
            st.file,
            st.batch_format_check,
            st.step_detail_format_check,
            st.created_at,
            e.request_id,
            e.case_study_name,
            e.batch_id,
            e.batch_name,
            e.step_id,
            e.step_name,
            e.job,
            e.step_detail_name
           FROM (mdm.dq_storage st
             LEFT JOIN cte_etl e ON ((((e.batch_name)::text = st.batch) AND (e.job = st.folder) AND ((e.step_detail_name)::text = st.file))))
        )
 SELECT cte.folder,
    cte.batch,
    cte.file
   FROM cte
  WHERE (cte.step_detail_name IS NULL);


ALTER TABLE mdm.check_file_not_on_step_detail OWNER TO dpadmin;

--
-- TOC entry 219 (class 1259 OID 17329)
-- Name: check_stepdetail_not_on_storage; Type: VIEW; Schema: mdm; Owner: dpadmin
--

CREATE VIEW mdm.check_stepdetail_not_on_storage AS
 WITH cte_etl AS (
         SELECT t1.request_id,
            t1.case_study_name,
            t2.batch_id,
            t2.batch_name,
            t3.step_id,
            t3.step_name,
            lower("substring"((t3.step_name)::text, 1, ("position"((t3.step_name)::text, '-'::text) - 1))) AS job,
            t4.step_detail_name
           FROM (((mdm.etl_request t1
             LEFT JOIN mdm.etl_company_batch t2 ON ((t1.request_id = t2.request_id)))
             LEFT JOIN mdm.etl_step t3 ON ((t3.batch_id = t2.batch_id)))
             LEFT JOIN mdm.etl_step_detail t4 ON ((t4.step_id = t3.step_id)))
          ORDER BY t1.request_id DESC, t2.batch_id DESC, t3.step_id, t4.step_detail_id
        ), cte AS (
         SELECT e.request_id,
            e.case_study_name,
            e.batch_id,
            e.batch_name,
            e.step_id,
            e.step_name,
            e.job,
            e.step_detail_name,
            st.file AS storage_file
           FROM (cte_etl e
             LEFT JOIN mdm.dq_storage st ON ((((e.batch_name)::text = st.batch) AND (e.job = st.folder) AND ((e.step_detail_name)::text = st.file))))
        )
 SELECT cte.request_id,
    cte.case_study_name,
    cte.batch_id,
    cte.batch_name,
    cte.step_id,
    cte.step_name,
    cte.step_detail_name
   FROM cte
  WHERE ((cte.storage_file IS NULL) AND (cte.step_detail_name IS NOT NULL));


ALTER TABLE mdm.check_stepdetail_not_on_storage OWNER TO dpadmin;

--
-- TOC entry 207 (class 1259 OID 16455)
-- Name: dq_checking_output; Type: TABLE; Schema: mdm; Owner: dpadmin
--

CREATE TABLE mdm.dq_checking_output (
    id integer NOT NULL,
    obj_name character varying(255),
    obj_table character varying(255),
    etl_obj_id integer,
    error_value character varying(255),
    rule_id integer,
    created_at timestamp without time zone
);


ALTER TABLE mdm.dq_checking_output OWNER TO dpadmin;

--
-- TOC entry 208 (class 1259 OID 16461)
-- Name: dq_checking_output_id_seq; Type: SEQUENCE; Schema: mdm; Owner: dpadmin
--

CREATE SEQUENCE mdm.dq_checking_output_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mdm.dq_checking_output_id_seq OWNER TO dpadmin;

--
-- TOC entry 3707 (class 0 OID 0)
-- Dependencies: 208
-- Name: dq_checking_output_id_seq; Type: SEQUENCE OWNED BY; Schema: mdm; Owner: dpadmin
--

ALTER SEQUENCE mdm.dq_checking_output_id_seq OWNED BY mdm.dq_checking_output.id;


--
-- TOC entry 209 (class 1259 OID 16463)
-- Name: dq_checking_rule; Type: TABLE; Schema: mdm; Owner: dpadmin
--

CREATE TABLE mdm.dq_checking_rule (
    id integer NOT NULL,
    rule_name character varying(255),
    obj_id integer,
    obj_name character varying(255),
    obj_type character varying(255),
    reason character varying(255),
    suggestion character varying(255),
    created_at timestamp without time zone,
    rule_id integer,
    level character varying(255)
);


ALTER TABLE mdm.dq_checking_rule OWNER TO dpadmin;

--
-- TOC entry 210 (class 1259 OID 16469)
-- Name: dq_checking_rule_id_seq; Type: SEQUENCE; Schema: mdm; Owner: dpadmin
--

CREATE SEQUENCE mdm.dq_checking_rule_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mdm.dq_checking_rule_id_seq OWNER TO dpadmin;

--
-- TOC entry 3708 (class 0 OID 0)
-- Dependencies: 210
-- Name: dq_checking_rule_id_seq; Type: SEQUENCE OWNED BY; Schema: mdm; Owner: dpadmin
--

ALTER SEQUENCE mdm.dq_checking_rule_id_seq OWNED BY mdm.dq_checking_rule.id;


--
-- TOC entry 211 (class 1259 OID 16471)
-- Name: dq_definition_rule; Type: TABLE; Schema: mdm; Owner: dpadmin
--

CREATE TABLE mdm.dq_definition_rule (
    id integer NOT NULL,
    rule_name character varying(255),
    reason character varying(255),
    level character varying(255),
    suggestion character varying(255),
    created_at timestamp without time zone
);


ALTER TABLE mdm.dq_definition_rule OWNER TO dpadmin;

--
-- TOC entry 212 (class 1259 OID 16477)
-- Name: dq_definition_rule_id_seq; Type: SEQUENCE; Schema: mdm; Owner: dpadmin
--

CREATE SEQUENCE mdm.dq_definition_rule_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mdm.dq_definition_rule_id_seq OWNER TO dpadmin;

--
-- TOC entry 3709 (class 0 OID 0)
-- Dependencies: 212
-- Name: dq_definition_rule_id_seq; Type: SEQUENCE OWNED BY; Schema: mdm; Owner: dpadmin
--

ALTER SEQUENCE mdm.dq_definition_rule_id_seq OWNED BY mdm.dq_definition_rule.id;


--
-- TOC entry 223 (class 1259 OID 17356)
-- Name: dq_file_structure; Type: TABLE; Schema: mdm; Owner: dpadmin
--

CREATE TABLE mdm.dq_file_structure (
    id integer NOT NULL,
    step character varying(255),
    batch_name character varying(255),
    source_name character varying(255),
    company_name character varying(255),
    batch_date character varying(255),
    file_name character varying(255),
    number_of_column integer,
    list_column_name character varying,
    non_nullable_columns character varying,
    number_of_row integer,
    number_dist_review integer,
    int_type_columns character varying,
    float_type_columns character varying,
    bool_type_columns character varying,
    str_type_columns character varying,
    other_type_columns character varying,
    inconsitent_type_columns character varying,
    datetime_type_columns character varying,
    datetime_format character varying,
    created_at timestamp without time zone
);


ALTER TABLE mdm.dq_file_structure OWNER TO dpadmin;

--
-- TOC entry 222 (class 1259 OID 17354)
-- Name: dq_file_structure_id_seq; Type: SEQUENCE; Schema: mdm; Owner: dpadmin
--

CREATE SEQUENCE mdm.dq_file_structure_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mdm.dq_file_structure_id_seq OWNER TO dpadmin;

--
-- TOC entry 3710 (class 0 OID 0)
-- Dependencies: 222
-- Name: dq_file_structure_id_seq; Type: SEQUENCE OWNED BY; Schema: mdm; Owner: dpadmin
--

ALTER SEQUENCE mdm.dq_file_structure_id_seq OWNED BY mdm.dq_file_structure.id;


--
-- TOC entry 213 (class 1259 OID 16479)
-- Name: dq_full_detail; Type: VIEW; Schema: mdm; Owner: dpadmin
--

CREATE VIEW mdm.dq_full_detail AS
 WITH cte_output_rule AS (
         SELECT o_1.id,
            o_1.obj_name,
            o_1.obj_table,
            o_1.etl_obj_id,
            o_1.error_value,
            o_1.rule_id,
            o_1.created_at,
            d.rule_name,
            d.reason,
            d.level,
            d.suggestion
           FROM (mdm.dq_checking_output o_1
             LEFT JOIN mdm.dq_definition_rule d ON ((o_1.rule_id = d.id)))
        )
 SELECT o.id,
    o.obj_name,
    o.obj_table,
    o.etl_obj_id,
    o.error_value,
    o.rule_id,
    o.created_at,
    o.rule_name,
    o.reason,
    o.level,
    o.suggestion,
    c.obj_type
   FROM (cte_output_rule o
     LEFT JOIN mdm.dq_checking_rule c ON ((o.rule_id = c.rule_id)));


ALTER TABLE mdm.dq_full_detail OWNER TO dpadmin;

--
-- TOC entry 224 (class 1259 OID 17541)
-- Name: etl_full_detail; Type: VIEW; Schema: mdm; Owner: dpadmin
--

CREATE VIEW mdm.etl_full_detail AS
 SELECT t1.case_study_name,
    t1.case_study_id AS request_case_study_id,
    t1.created_at AS request_created_at,
    t1.updated_at AS request_updated_at,
    t1.status AS request_status,
    t2.batch_name,
    t2.created_at AS batch_created_at,
    t2.updated_at AS batch_updated_at,
    t2.status AS batch_status,
    t2.source_name,
    t2.url,
    t3.step_name,
    t3.step_type,
    t3.created_at AS step_created_at,
    t3.updated_at AS step_updated_at,
    t3.status AS step_status,
    t4.step_detail_name,
    t4.created_at AS step_detail_created_at,
    t4.updated_at AS step_detail_updated_at,
    t4.status AS step_detail_status,
    t4.paging,
    t4.progress_current,
    t4.progress_total,
    t4.item_count,
    t4.step_detail_id,
    t1.request_id,
    t2.batch_id,
    t3.step_id
   FROM (((mdm.etl_request t1
     LEFT JOIN mdm.etl_company_batch t2 ON ((t1.request_id = t2.request_id)))
     LEFT JOIN mdm.etl_step t3 ON ((t3.batch_id = t2.batch_id)))
     LEFT JOIN mdm.etl_step_detail t4 ON ((t4.step_id = t3.step_id)))
  ORDER BY t1.request_id DESC, t2.batch_id DESC, t3.step_id, t4.step_detail_id;


ALTER TABLE mdm.etl_full_detail OWNER TO dpadmin;

--
-- TOC entry 217 (class 1259 OID 17304)
-- Name: etl_request_request_id_seq; Type: SEQUENCE; Schema: mdm; Owner: dpadmin
--

ALTER TABLE mdm.etl_request ALTER COLUMN request_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME mdm.etl_request_request_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 216 (class 1259 OID 16501)
-- Name: etl_step_detail_step_detail_id_seq; Type: SEQUENCE; Schema: mdm; Owner: dpadmin
--

CREATE SEQUENCE mdm.etl_step_detail_step_detail_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mdm.etl_step_detail_step_detail_id_seq OWNER TO dpadmin;

--
-- TOC entry 3711 (class 0 OID 0)
-- Dependencies: 216
-- Name: etl_step_detail_step_detail_id_seq; Type: SEQUENCE OWNED BY; Schema: mdm; Owner: dpadmin
--

ALTER SEQUENCE mdm.etl_step_detail_step_detail_id_seq OWNED BY mdm.etl_step_detail.step_detail_id;


--
-- TOC entry 221 (class 1259 OID 17339)
-- Name: etl_storage_full_detail; Type: VIEW; Schema: mdm; Owner: dpadmin
--

CREATE VIEW mdm.etl_storage_full_detail AS
 WITH cte_etl AS (
         SELECT t1.request_id,
            t1.case_study_name,
            t2.batch_id,
            t2.batch_name,
            t3.step_id,
            t3.step_name,
            t3.step_type,
            t4.step_detail_name
           FROM (((mdm.etl_request t1
             LEFT JOIN mdm.etl_company_batch t2 ON ((t1.request_id = t2.request_id)))
             LEFT JOIN mdm.etl_step t3 ON ((t3.batch_id = t2.batch_id)))
             LEFT JOIN mdm.etl_step_detail t4 ON ((t4.step_id = t3.step_id)))
          ORDER BY t1.request_id DESC, t2.batch_id DESC, t3.step_id, t4.step_detail_id
        ), cte AS (
         SELECT e.request_id,
            e.case_study_name,
            e.batch_id,
            e.batch_name,
            e.step_id,
            e.step_name,
            e.step_type,
            e.step_detail_name,
            st.file AS storage_file,
            st.folder,
            st.batch
           FROM (cte_etl e
             LEFT JOIN mdm.dq_storage st ON ((((e.batch_name)::text = st.batch) AND ((e.step_type)::text = st.folder))))
        )
 SELECT cte.request_id,
    cte.case_study_name,
    cte.batch_id,
    cte.batch_name,
    cte.step_id,
    cte.step_name,
    cte.step_detail_name,
    cte.storage_file,
    cte.folder,
    cte.batch AS storage_batch,
    cte.step_type
   FROM cte
  ORDER BY cte.request_id;


ALTER TABLE mdm.etl_storage_full_detail OWNER TO dpadmin;

--
-- TOC entry 3520 (class 2604 OID 16503)
-- Name: dq_checking_output id; Type: DEFAULT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_checking_output ALTER COLUMN id SET DEFAULT nextval('mdm.dq_checking_output_id_seq'::regclass);


--
-- TOC entry 3521 (class 2604 OID 16504)
-- Name: dq_checking_rule id; Type: DEFAULT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_checking_rule ALTER COLUMN id SET DEFAULT nextval('mdm.dq_checking_rule_id_seq'::regclass);


--
-- TOC entry 3522 (class 2604 OID 16505)
-- Name: dq_definition_rule id; Type: DEFAULT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_definition_rule ALTER COLUMN id SET DEFAULT nextval('mdm.dq_definition_rule_id_seq'::regclass);


--
-- TOC entry 3526 (class 2604 OID 17359)
-- Name: dq_file_structure id; Type: DEFAULT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_file_structure ALTER COLUMN id SET DEFAULT nextval('mdm.dq_file_structure_id_seq'::regclass);


--
-- TOC entry 3518 (class 2604 OID 16506)
-- Name: etl_company_batch batch_id; Type: DEFAULT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_company_batch ALTER COLUMN batch_id SET DEFAULT nextval('mdm."EtlCompanyBatch_batch_id_seq"'::regclass);


--
-- TOC entry 3519 (class 2604 OID 16509)
-- Name: etl_step step_id; Type: DEFAULT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_step ALTER COLUMN step_id SET DEFAULT nextval('mdm."EtlStep_step_id_seq"'::regclass);


--
-- TOC entry 3523 (class 2604 OID 16510)
-- Name: etl_step_detail step_detail_id; Type: DEFAULT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_step_detail ALTER COLUMN step_detail_id SET DEFAULT nextval('mdm.etl_step_detail_step_detail_id_seq'::regclass);


--
-- TOC entry 3541 (class 2606 OID 17315)
-- Name: etl_request case_study_id; Type: CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_request
    ADD CONSTRAINT case_study_id UNIQUE (case_study_id);


--
-- TOC entry 3532 (class 2606 OID 16514)
-- Name: dq_checking_output dq_checking_output_pkey; Type: CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_checking_output
    ADD CONSTRAINT dq_checking_output_pkey PRIMARY KEY (id);


--
-- TOC entry 3534 (class 2606 OID 16516)
-- Name: dq_checking_rule dq_checking_rule_pkey; Type: CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_checking_rule
    ADD CONSTRAINT dq_checking_rule_pkey PRIMARY KEY (id);


--
-- TOC entry 3536 (class 2606 OID 16518)
-- Name: dq_definition_rule dq_definition_rule_pkey; Type: CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_definition_rule
    ADD CONSTRAINT dq_definition_rule_pkey PRIMARY KEY (id);


--
-- TOC entry 3528 (class 2606 OID 16520)
-- Name: etl_company_batch etl_company_batch_pkey; Type: CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_company_batch
    ADD CONSTRAINT etl_company_batch_pkey PRIMARY KEY (batch_id);


--
-- TOC entry 3543 (class 2606 OID 17313)
-- Name: etl_request etl_request_pkey; Type: CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_request
    ADD CONSTRAINT etl_request_pkey PRIMARY KEY (request_id);


--
-- TOC entry 3539 (class 2606 OID 16522)
-- Name: etl_step_detail etl_step_detail_pkey; Type: CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_step_detail
    ADD CONSTRAINT etl_step_detail_pkey PRIMARY KEY (step_detail_id);


--
-- TOC entry 3530 (class 2606 OID 16524)
-- Name: etl_step etl_step_pkey; Type: CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_step
    ADD CONSTRAINT etl_step_pkey PRIMARY KEY (step_id);


--
-- TOC entry 3537 (class 1259 OID 16525)
-- Name: ix_mdm_dq_storage_id; Type: INDEX; Schema: mdm; Owner: dpadmin
--

CREATE INDEX ix_mdm_dq_storage_id ON mdm.dq_storage USING btree (id);


--
-- TOC entry 3544 (class 1259 OID 17571)
-- Name: status_index; Type: INDEX; Schema: mdm; Owner: dpadmin
--

CREATE INDEX status_index ON mdm.etl_request USING btree (status);


--
-- TOC entry 3557 (class 2620 OID 16601)
-- Name: etl_step rule1; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--




--
-- TOC entry 3552 (class 2620 OID 16620)
-- Name: etl_company_batch rule11; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--



--
-- TOC entry 3553 (class 2620 OID 16622)
-- Name: etl_company_batch rule12; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--


--
-- TOC entry 3554 (class 2620 OID 16624)
-- Name: etl_company_batch rule13; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--




--
-- TOC entry 3555 (class 2620 OID 16626)
-- Name: etl_company_batch rule14; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--



--
-- TOC entry 3556 (class 2620 OID 16628)
-- Name: etl_company_batch rule15; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--




--
-- TOC entry 3563 (class 2620 OID 16630)
-- Name: etl_step_detail rule16; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--



--
-- TOC entry 3560 (class 2620 OID 16632)
-- Name: etl_step rule17; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--




--
-- TOC entry 3565 (class 2620 OID 16638)
-- Name: etl_step_detail rule18; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--




--
-- TOC entry 3561 (class 2620 OID 16635)
-- Name: etl_step rule19; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--



--
-- TOC entry 3549 (class 2620 OID 16603)
-- Name: etl_company_batch rule2; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--




--
-- TOC entry 3564 (class 2620 OID 16637)
-- Name: etl_step_detail rule20; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--



--
-- TOC entry 3558 (class 2620 OID 16607)
-- Name: etl_step rule4; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--



--
-- TOC entry 3550 (class 2620 OID 16609)
-- Name: etl_company_batch rule5; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--



--
-- TOC entry 3562 (class 2620 OID 16599)
-- Name: etl_step_detail rule7; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--




--
-- TOC entry 3559 (class 2620 OID 16614)
-- Name: etl_step rule8; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--



--
-- TOC entry 3551 (class 2620 OID 16616)
-- Name: etl_company_batch rule9; Type: TRIGGER; Schema: mdm; Owner: dpadmin
--




--
-- TOC entry 3546 (class 2606 OID 16526)
-- Name: dq_checking_output dq_checking_output_rule_id_fkey; Type: FK CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_checking_output
    ADD CONSTRAINT dq_checking_output_rule_id_fkey FOREIGN KEY (rule_id) REFERENCES mdm.dq_definition_rule(id) NOT VALID;


--
-- TOC entry 3547 (class 2606 OID 16531)
-- Name: dq_checking_rule dq_checking_rule_rule_id_fkey; Type: FK CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.dq_checking_rule
    ADD CONSTRAINT dq_checking_rule_rule_id_fkey FOREIGN KEY (rule_id) REFERENCES mdm.dq_definition_rule(id) NOT VALID;


--
-- TOC entry 3545 (class 2606 OID 16541)
-- Name: etl_step etl_step_batch_id_fkey; Type: FK CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_step
    ADD CONSTRAINT etl_step_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES mdm.etl_company_batch(batch_id) NOT VALID;


--
-- TOC entry 3548 (class 2606 OID 16546)
-- Name: etl_step_detail etl_step_detail_step_id_fkey; Type: FK CONSTRAINT; Schema: mdm; Owner: dpadmin
--

ALTER TABLE ONLY mdm.etl_step_detail
    ADD CONSTRAINT etl_step_detail_step_id_fkey FOREIGN KEY (step_id) REFERENCES mdm.etl_step(step_id) NOT VALID;

--
-- TOC entry 1785 (class 826 OID 16551)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: mdm; Owner: dpadmin
--

ALTER DEFAULT PRIVILEGES FOR ROLE dpadmin IN SCHEMA mdm REVOKE ALL ON TABLES  FROM dpadmin;
ALTER DEFAULT PRIVILEGES FOR ROLE dpadmin IN SCHEMA mdm GRANT SELECT ON TABLES  TO dpadmin;


-- Completed on 2021-01-06 17:58:13 +07

--
-- PostgreSQL database dump complete
--
"""
