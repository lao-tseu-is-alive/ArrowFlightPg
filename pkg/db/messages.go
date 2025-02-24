package db

const (
	FieldCannotBeEmpty         = "field %s cannot be empty or contain only spaces"
	FieldMinLengthIsN          = "field %s minimum length is %d"
	FoundNum                   = ", found %d"
	FunctionNReturnedNoResults = "%s returned no results "
	SelectFailedInNWithErrorE  = "pgxscan.Select unexpectedly failed in %s, error : %v"
)
