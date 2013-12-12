Module Pi

    Sub Main()
        Dim env As MPI.Environment = New MPI.Environment(Split(Command$, " "))
        Dim dartsPerProcessor As Integer = 10000
        Dim world As MPI.Communicator = MPI.Communicator.world

        If Command$() <> "" Then
            dartsPerProcessor = System.Convert.ToInt32(Command$)
        End If

        Dim i As Integer

        Dim dartsInCircle As Integer = 0
        Dim random As System.Random = New System.Random(world.Rank * 5)
        For i = 0 To dartsPerProcessor - 1
            Dim x As Double = (random.NextDouble() - 0.5) * 2.0
            Dim y As Double = (random.NextDouble() - 0.5) * 2.0
            If x * x + y * y <= 1.0 Then
                dartsInCircle = dartsInCircle + 1
            End If
        Next

        If world.Rank = 0 Then
            Dim totalDartsInCircle As Integer
            world.Reduce(Of Integer)(dartsInCircle, totalDartsInCircle, MPI.Operation(Of Integer).Add, 0)
            Dim result As Double = 4.0 * totalDartsInCircle / (world.Size * dartsPerProcessor)
            System.Console.WriteLine("Pi is approximately {0:F15}.", result)
        Else
            world.Reduce(Of Integer)(dartsInCircle, MPI.Operation(Of Integer).Add, 0)
        End If

        ' Shut down MPI. 
        env.Dispose()
    End Sub

End Module
