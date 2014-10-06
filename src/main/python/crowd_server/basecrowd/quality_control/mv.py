# Majority Vote: select the most common answer for each record in a task.
def make_mv_answer(task_obj):
    answers = []

    responses = task_obj.responses.all()
    for response in responses :
        current_content = response.content.split(",")
        answers.append(current_content)

    if (len(answers) == 0) :
        return ''

    mv_answer = []
    # For each record
    for i in range(len(answers[0])) :
        count = {}
        # For each assignment
        for j in range(len(answers)) :
            if answers[j][i] in count :
                count[answers[j][i]] += 1
            else :
                count[answers[j][i]] = 1

        # Find the mode
        current_answer = ''
        max_count = -1
        for key, value in count.iteritems() :
            if (value > max_count) :
                max_count = value
                current_answer = key
        mv_answer.append(current_answer)

    return ','.join(mv_answer)
