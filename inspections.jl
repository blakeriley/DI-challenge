using DataFrames
using DataFramesMeta
using Base.Dates
using Loess

DateTime("3/27/2008 8:31", "m/d/y H:M")

df = readtable("Food_Establishment_Inspections.csv")
df = df[complete_cases(@select(df,:LICENSENO,:ISSDTTM,
                                  :EXPDTTM,:RESULTDTTM)), :]

@inline dateorna(col) = isna(col) ? NA : Date(col, "m/d/y H:M")

df = @byrow! df begin
    @newcol IssueDate::DataArray{Date}
    @newcol ExpDate::DataArray{Date}
    @newcol ResultDate::DataArray{Date}
    @newcol ViolDate::DataArray{Date}

    :IssueDate = dateorna(:ISSDTTM)
    :ExpDate = dateorna(:EXPDTTM)
    :ResultDate = dateorna(:RESULTDTTM)
    :ViolDate = dateorna(:VIOLDTTM)
end

active = by(@where(df, :RESULT .!= "HE_NotReq"), :LICENSENO) do d
    DataFrame(min = isempty(d[:ResultDate]) ? NA : minimum(d[:ResultDate]),
              max = isempty(d[:ResultDate]) ? NA : maximum(d[:ResultDate]),
              ExpDate = d[1,:ExpDate])
end

active = active[complete_cases(active),:]

beginning = minimum(active[:min])
range = beginning:Date(now())

netactive = fill(0, convert(Int, Date(now()) - beginning) + 1)
@byrow! active begin
    netactive[convert(Int, :min - beginning) + 1] += 1
    if :ExpDate < Date(now())
        netactive[convert(Int, :max - beginning) + 1] += -1
    end
end

violations = by(@where(df, (:RESULT .== "HE_Fail") | ((:RESULT .== "HE_Pass") & isna(:Violation))), :LICENSENO) do d
    by(d, :ResultDate) do e
        DataFrame(NumViol = count(stat -> !isna(stat), e[:ViolStatus]))
    end
end
violations = sort(violations, cols=[:LICENSENO, :ResultDate])
violations[:TimeSince] = @data(fill(0, size(violations)[1]))
violations[1, :TimeSince] = NA
for i=2:size(violations)[1]
    if violations[i, :LICENSENO] == violations[i-1, :LICENSENO]
        violations[i, :TimeSince] = (violations[i, :ResultDate] -
                                     violations[i-1, :ResultDate]).value
    else
        violations[i, :TimeSince] = NA
    end
end

function plot_numviol()
    numdays = 365*3
    xs = convert(Array{Float64},
                 violations[complete_cases(violations), :TimeSince])
    ys = convert(Array{Float64},
                 violations[complete_cases(violations), :NumViol])
    ys = ys[xs .< numdays]
    xs = xs[xs .< numdays]
    
    model = loess(xs, ys)
    us = collect(1:maximum(xs))
    vs = Loess.predict(model, us)

    plotfile("numviol.html")
    plot(xs,
         ys - .25 .+ .5*rand(length(xs)),
         Glyph(:Circle, fillcolor="rgba(0, 120, 0, 0.2)",
               size=2, linecolor="transparent"),
         title = "Days Between Inspections vs. Number of Violations")
    hold(true)
    plot(us, vs, Glyph(:Line, linewidth=3, linecolor="blue"))
    showplot()
    hold(false)
end

function plot_timebetweensucc()

    pairs = DataFrame(t1 = violations[1:(size(violations)[1]-1), :TimeSince],
                      t2 = violations[2:size(violations)[1], :TimeSince])
    pairs = pairs[complete_cases(pairs),:]
    

    numdays = 365*3
    xs = convert(Array{Float64}, pairs[:t1])
    ys = convert(Array{Float64}, pairs[:t2])
    ys = ys[xs .< numdays]
    xs = xs[xs .< numdays]
    
    model = loess(xs, ys)
    us = collect(1:maximum(xs))
    vs = Loess.predict(model, us)

    plotfile("timebetweensucc.html")
    plot(xs,
         ys,
         Glyph(:Circle, fillcolor="rgba(0, 100, 0, 0.2)",
               size=2, linecolor="transparent"),
         title="Days Between Prior Inspection vs Days Between Next Inspection")
    hold(true)
    plot(us, vs, Glyph(:Line, linewidth=3, linecolor="blue"))
    showplot()
    hold(false)
end

plot_numviol()
plot_timebetweensucc()
