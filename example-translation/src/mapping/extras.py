# Extra ontology properties for Help Seeker

from owlready2 import DataProperty, AnnotationProperty


class hasClaimingStatus(DataProperty):
    # An indicator that can capture whether the organization claimed their listings in HelpSeeker SM.
    # Can be "Claimed" or "Unclaimed"
    pass


class hasSMType(DataProperty):
    # Can be "Benefit", "Helpline", "Program", "Location"
    pass


class hasUUID(AnnotationProperty):
    # Sotres the UUID for HelpSeeker application
    pass
