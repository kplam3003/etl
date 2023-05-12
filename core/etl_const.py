import enum

class Meta_DataType(enum.Enum):
    REVIEW_STATS = 'review_stats'
    REVIEW = 'review'
    JOB = 'job'
    OVERVIEW = 'overview'
    CORESIGNAL_STATS = 'coresignal_stats'
    CORESIGNAL_EMPLOYEES = 'coresignal_employees'

class Meta_NLPType(enum.Enum):
    VOC = 'VoC'
    VOE = 'VoE'
    HR = 'HRA'

class Meta_JobType(enum.Enum):
    FULL_TIME = 'full_time'
    PART_TIME = 'part_time'
    INTERNSHIP = 'internship'
    TEMPORARY = 'temporary'
    CONTRACT = 'contract'
    PERMANET = 'permanet'
    VOLUNTEER = 'volunteer'
    APPRENTICESHIP = 'apprenticeship'