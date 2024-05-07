package dpfm_api_output_formatter

import (
	"data-platform-api-participation-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToHeader(rows *sql.Rows) (*[]Header, error) {
	defer rows.Close()
	header := make([]Header, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Header{}

		err := rows.Scan(
			&pm.Participation,
			&pm.ParticipationDate,
			&pm.ParticipationTime,
			&pm.Participator,
			&pm.ParticipationObjectType,
			&pm.ParticipationObject,
			&pm.Attendance,
			&pm.CreationDate,
			&pm.CreationTime,
			&pm.IsCancelled,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &header, err
		}

		data := pm
		header = append(header, Header{
			Participation:				data.Participation,
			ParticipationDate:			data.ParticipationDate,
			ParticipationTime:			data.ParticipationTime,
			Participator:				data.Participator,
			ParticipationObjectType:	data.ParticipationObjectType,
			ParticipationObject:		data.ParticipationObject,
			Attendance:					data.Attendance,
			CreationDate:				data.CreationDate,
			CreationTime:				data.CreationTime,
			IsCancelled:				data.IsCancelled,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &header, nil
	}

	return &header, nil
}
